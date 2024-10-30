from package.cli.dbt_docs_cli import docs_app
from package.cli.root import app
from package.config.constants import CODEGEN_TO_CLICKHOUSE_DATA_TYPES
from package.project import Project
from package.utils.dbt_utils import find_model_sql
from package.utils.filesystem import find_up, get_file_name
from pathlib import Path
from typing import Optional

import dbt.version
import json
import os
import subprocess
import typer
import yaml

dbt_app = typer.Typer(name="dbt", add_completion=False)
dbt_app.add_typer(docs_app)
app.add_typer(dbt_app)


@dbt_app.command(help="Print version.")
def version():
    app.console.print(dbt.version.__version__)


@dbt_app.command(help="Generate test fixtures.")
def fixtures(
    models: Optional[list[str]] = typer.Option(None, "-m", "--model", help="1 or more model names"),
):
    cwd = os.getcwd()
    dbt_project_file = find_up(cwd, "dbt_project.yml")

    if not dbt_project_file:
        raise Exception(f"No dbt_project.yml found in {cwd} or higher")

    project = Project.from_path(cwd)

    if models:
        patterns = [os.path.join("fixtures", f"{model}.py") for model in models]
    else:
        patterns = [os.path.join("fixtures", "*.py")]

    script_paths = [
        file for pattern in patterns for file in project.dbt_tests_directory.glob(pattern)
    ]

    for script_path in script_paths:
        try:
            cmd = ["python", script_path]
            subprocess.check_output(cmd, cwd=project.dbt_directory)
            app.console.print(f"Generated test fixtures from {script_path}", style="green")
        except subprocess.CalledProcessError as exc:
            output = exc.output.decode().strip()
            app.console.print(output, style="red")
            raise exc


@dbt_app.command(help="Generate model YAML.")
def model_yaml(models: list[str]):
    cwd = os.getcwd()
    dbt_project_file = find_up(cwd, "dbt_project.yml")

    if not dbt_project_file:
        raise Exception(f"No dbt_project.yml found in {cwd} or higher")

    project = Project.from_path(cwd)
    model_paths = []
    for model in models:
        model_paths.extend(find_model_sql(project, model))

    for model_path in model_paths:
        model_name = get_file_name(model_path)
        schema_file = os.path.join(Path(model_path).parent, "schema", f"{model_name}.yml")
        schema_dir = os.path.dirname(schema_file)

        os.makedirs(schema_dir, exist_ok=True)

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
            subprocess.check_output(cmd, cwd=project.dbt_directory)
        except subprocess.CalledProcessError as exc:
            output = exc.output.decode().strip()
            app.console.print(output, style="red")
            raise exc

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
            output = subprocess.check_output(cmd, cwd=project.dbt_directory).decode().strip()
        except subprocess.CalledProcessError as exc:
            output = exc.output.decode().strip()
            app.console.print(output, style="red")
            raise exc

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
