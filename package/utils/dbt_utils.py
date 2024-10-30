from package.project import Project
from package.utils.filesystem import rmtree
from package.utils.pydantic_utils import dump_csv
from pathlib import Path
from prefect_shell.commands import ShellOperation
from typing import Any, Optional

import json
import os
import re

RE_REF = r"^ref\(['\"](.*?)['\"]\)$"


def extract_model_name(string: str) -> str | None:
    """Extract the model name from a string that contains a dbt ref function."""
    matches = re.search(RE_REF, string)

    if matches:
        return matches.group(1)
    else:
        return None


def find_model_sql(project: Project, model: str):
    model_paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.sql")))

    # TODO Raise exception if multiple matches
    return model_paths[:1]


def find_model_yaml(project: Project, model: str):
    model_paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.yml")))

    # TODO Raise exception if multiple matches
    return model_paths[:1]


def dump_test_fixtures(project: Project, fixtures: list[dict]):
    """Dump test fixtures to CSV."""
    fixtures_path = os.path.join(project.dbt_tests_directory, "fixtures")

    for fixture in fixtures:
        fixture_path = os.path.join(fixtures_path, fixture["model"])
        rmtree(fixture_path)
        os.makedirs(fixture_path, exist_ok=True)

        # Given
        for value in fixture["given"]:
            model_name = extract_model_name(value["input"])
            csv_filename = f'{fixture["model"]}__{fixture["name"]}__{model_name}.csv'
            csv_path = os.path.join(fixture_path, csv_filename)
            csv_data = dump_csv(*value["data"])

            with open(csv_path, "wt") as fp:
                fp.write(csv_data)

        # Expected
        csv_filename = f'{fixture["model"]}__{fixture["name"]}__expect.csv'
        csv_path = os.path.join(fixture_path, csv_filename)
        csv_data = dump_csv(*fixture["expect"]["data"])

        with open(csv_path, "wt") as fp:
            fp.write(csv_data)


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
