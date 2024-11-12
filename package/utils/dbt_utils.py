from package.config.constants import DBT_PROFILES_DIR
from package.project import Project
from package.utils.yaml_utils import safe_load_file
from pathlib import Path
from prefect_shell.commands import ShellOperation
from typing import Any, List, Optional

import json
import os
import pydash
import subprocess

RE_REF = r"^ref\(['\"](.*?)['\"]\)$"
RE_SOURCE = r"^source\(['\"](.*?)['\"], ['\"](.*?)['\"]\)$"


def find_model_sql(project: Project, model: str) -> str | None:
    paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.sql")))

    if paths:
        # TODO Raise exception if multiple matches
        return paths[0]
    else:
        return None


def list_resources(project_dir: Path | str, resource_type: Optional[str] = None) -> List[dict]:
    result = []

    cmd = dbt_list_command(
        profiles_dir=DBT_PROFILES_DIR,
        project_dir=project_dir,
        resource_type=resource_type,
        output="json",
    )
    stdout = run_command(cmd)

    for line in stdout.splitlines():
        try:
            data = json.loads(line)
        except json.decoder.JSONDecodeError:
            continue

        result.append(data)

    # Add original config not returned by `dbt list` command
    if resource_type == "source":
        file_paths = pydash.uniq([resource["original_file_path"] for resource in result])
        files = {
            file_path: safe_load_file(os.path.join(project_dir, file_path))
            for file_path in file_paths
        }

        for resource in result:
            original_config = None
            data = files[resource["original_file_path"]]

            for source in data["sources"]:
                if source["name"] == resource["source_name"]:
                    for table in source["tables"]:
                        if table["name"] == resource["name"]:
                            original_config = table
                            break
                if original_config:
                    break

            resource["original_config"] = original_config

    return result


def run_command(command):
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Command failed with error:\n{e.stderr}"


def dbt_list_command_args(
    fail_fast=True,
    use_colors=False,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    output: Optional[str] = None,
    resource_type: Optional[str] = None,
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

    if output:
        args.extend(["--output", output])

    if resource_type:
        args.extend(["--resource-type", resource_type])

    if select:
        args.extend(["--select", select])

    if selector:
        args.extend(["--selector", selector])

    if target:
        args.extend(["--target", target])

    if vars:
        args.extend(["--vars", f"'{json.dumps(vars)}'"])

    return args


def dbt_list_command(
    profiles_dir: Path | str,
    project_dir: Path | str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    output: Optional[str] = None,
    resource_type: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[dict[str, Any]] = None,
) -> list[str]:
    cmd = ["dbt", "list", "--profiles-dir", str(profiles_dir), "--project-dir", str(project_dir)]
    cmd.extend(
        dbt_list_command_args(
            exclude=exclude,
            models=models,
            output=output,
            resource_type=resource_type,
            select=select,
            selector=selector,
            target=target,
            vars=vars,
        )
    )

    return cmd


async def dbt_list(
    profiles_dir: str,
    project_dir: str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    output: Optional[str] = None,
    resource_type: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[dict[str, Any]] = None,
) -> str:
    cmd = dbt_list_command(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        exclude=exclude,
        models=models,
        output=output,
        resource_type=resource_type,
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
