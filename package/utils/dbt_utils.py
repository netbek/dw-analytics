from package.project import Project
from package.utils.filesystem import rmtree
from package.utils.pydantic_utils import dump_csv
from package.utils.yaml_utils import PrettySafeDumper
from pathlib import Path
from prefect_shell.commands import ShellOperation
from typing import Any, Optional

import json
import os
import pydash
import re
import yaml

RE_REF = r"^ref\(['\"](.*?)['\"]\)$"
RE_SOURCE = r"^source\(['\"](.*?)['\"], ['\"](.*?)['\"]\)$"


def extract_table_name(string: str) -> str | None:
    """Extract the table name from a string that contains a dbt ref or source function."""
    matches = re.search(RE_REF, string)

    if matches:
        return matches.group(1)

    matches = re.search(RE_SOURCE, string)

    if matches:
        return matches.group(2)

    return None


def find_model_sql(project: Project, model: str) -> str | None:
    paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.sql")))

    if paths:
        # TODO Raise exception if multiple matches
        return paths[0]
    else:
        return None


def find_model_yaml(project: Project, model: str) -> str | None:
    paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.yml")))

    if paths:
        # TODO Raise exception if multiple matches
        return paths[0]
    else:
        return None


def dump_fixtures_csv(project: Project, fixtures: list[dict]):
    fixtures_path = os.path.join(project.dbt_tests_directory, "fixtures")

    for model_name, model_fixtures in pydash.group_by(fixtures, "model").items():
        for fixture in model_fixtures:
            fixture_path = os.path.join(fixtures_path, model_name)

            rmtree(fixture_path)
            os.makedirs(fixture_path, exist_ok=True)

            # Given
            for value in fixture["given"]:
                input_name = extract_table_name(value["input"])
                csv_filename = f'{model_name}__{fixture["name"]}__{input_name}.csv'
                csv_path = os.path.join(fixture_path, csv_filename)
                csv_data = dump_csv(*value["data"])

                with open(csv_path, "wt") as fp:
                    fp.write(csv_data)

            # Expected
            csv_filename = f'{model_name}__{fixture["name"]}__expect.csv'
            csv_path = os.path.join(fixture_path, csv_filename)
            csv_data = dump_csv(*fixture["expect"]["data"])

            with open(csv_path, "wt") as fp:
                fp.write(csv_data)


def update_model_unit_tests(project: Project, fixtures: list[dict]):
    for model_name, model_fixtures in pydash.group_by(fixtures, "model").items():
        schema_path = find_model_yaml(project, model_name)

        if not schema_path:
            continue

        unit_tests = []
        for fixture in model_fixtures:
            unit_test = {
                "model": model_name,
                "name": fixture["name"],
            }

            # Given
            unit_test["given"] = []
            for value in fixture["given"]:
                input_name = extract_table_name(value["input"])
                unit_test["given"].append(
                    {
                        "input": value["input"],
                        "format": value["format"],
                        "fixture": f'{model_name}__{fixture["name"]}__{input_name}',
                    }
                )

            # Expected
            unit_test["expect"] = {
                "format": value["format"],
                "fixture": f'{model_name}__{fixture["name"]}__expect',
            }

            unit_tests.append(unit_test)

        with open(schema_path, "rt") as fp:
            schema = yaml.safe_load(fp) or {}

        schema["unit_tests"] = unit_tests
        data = yaml.dump(schema, sort_keys=False, Dumper=PrettySafeDumper)

        with open(schema_path, "wt") as fp:
            fp.write(data)


def export_fixtures(project: Project, fixtures: list[dict]):
    dump_fixtures_csv(project, fixtures)
    update_model_unit_tests(project, fixtures)


def dbt_run_command_args(
    fail_fast=True,
    use_colors=False,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[dict[str, Any]] = None,
) -> list[str]:
    args = []

    if fail_fast:
        args.extend(["--fail-fast"])
    else:
        args.extend(["--no-fail-fast"])

    if use_colors:
        args.extend(["--use-colors"])
    else:
        args.extend(["--no-use-colors"])

    if exclude:
        args.extend(["--exclude", exclude])

    if models:
        args.extend(["--models", models])

    if select:
        args.extend(["--select", select])

    if selector:
        args.extend(["--selector", selector])

    if target:
        args.extend(["--target", target])

    if vars:
        args.extend(["--vars", f"'{json.dumps(vars)}'"])

    return args


def dbt_run_command(
    profiles_dir: Path | str,
    project_dir: Path | str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[dict[str, Any]] = None,
) -> list[str]:
    cmd = ["dbt", "run", "--profiles-dir", str(profiles_dir), "--project-dir", str(project_dir)]
    cmd.extend(
        dbt_run_command_args(
            exclude=exclude,
            models=models,
            select=select,
            selector=selector,
            target=target,
            vars=vars,
        )
    )

    return cmd


async def dbt_run(
    profiles_dir: str,
    project_dir: str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[dict[str, Any]] = None,
) -> str:
    cmd = dbt_run_command(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        exclude=exclude,
        models=models,
        select=select,
        selector=selector,
        target=target,
        vars=vars,
    )

    async with ShellOperation(commands=[" ".join(cmd)], working_dir=project_dir) as op:
        process = await op.trigger()
        await process.wait_for_completion()
        result = await process.fetch_result()

    return result
