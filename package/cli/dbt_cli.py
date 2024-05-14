from package.cli.root import app
from package.constants import CODEGEN_TO_CLICKHOUSE_DATA_TYPES
from package.utils.filesystem import find_up
from pathlib import Path
from typing import List

import dbt.version
import json
import os
import subprocess
import typer
import yaml

dbt_app = typer.Typer(name="dbt", add_completion=False)
app.add_typer(dbt_app)


@dbt_app.command()
def version():
    print(dbt.version.__version__)


@dbt_app.command()
def generate_model_yaml(models: List[str]):
    cwd = os.getcwd()
    dbt_project_file = find_up(cwd, "dbt_project.yml")

    if not dbt_project_file:
        raise Exception(f"No dbt_project.yml found in {cwd} or higher")

    dbt_dir = dbt_project_file.parent
    patterns = [f"models/**/{model}.sql" for model in models]
    model_paths = [file for pattern in patterns for file in dbt_dir.glob(pattern)]

    for model_path in model_paths:
        model_name = os.path.splitext(os.path.basename(model_path))[0]
        schema_file = os.path.join(Path(model_path).parent, "schema.yml")

        # Load existing schema
        if os.path.exists(schema_file):
            with open(schema_file, "rt") as file:
                schema = yaml.safe_load(file)
        else:
            schema = {"version": 2, "models": []}

        # Build model
        cmd = [
            "dbt",
            "--fail-fast",
            "--quiet",
            "run",
            "-m",
            model_name,
            "--full-refresh",
        ]
        try:
            subprocess.check_output(cmd, cwd=dbt_dir)
        except subprocess.CalledProcessError as e:
            output = e.output.decode().strip()
            print(output)
            raise e

        # Generate schema YAML
        cmd = [
            "dbt",
            "--quiet",
            "run-operation",
            "generate_model_yaml",
            "--args",
            json.dumps({"model_names": [model_name]}),
        ]
        try:
            output = subprocess.check_output(cmd, cwd=dbt_dir).decode().strip()
        except subprocess.CalledProcessError as e:
            output = e.output.decode().strip()
            print(output)
            raise e

        new_model = yaml.safe_load(output)["models"][0]
        new_model = {
            "name": new_model["name"],
            "columns": [
                {
                    "name": column["name"],
                    "data_type": CODEGEN_TO_CLICKHOUSE_DATA_TYPES.get(
                        column["data_type"], column["data_type"]
                    ),
                }
                for column in new_model["columns"]
            ],
        }

        old_model_indexes = [
            i for i, model in enumerate(schema["models"]) if model["name"] == model_name
        ]

        if old_model_indexes:
            schema["models"][old_model_indexes[0]] = new_model
        else:
            schema["models"].append(new_model)

        schema["models"] = sorted(schema["models"], key=lambda x: x["name"])

        # Write schema file
        with open(schema_file, "wt") as file:
            data = yaml.safe_dump(schema, sort_keys=False)
            file.write(data)
