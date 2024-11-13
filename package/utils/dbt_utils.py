from package.config.constants import DBT_PROFILES_DIR
from package.project import Project
from package.types import DbtModel, DbtResourceType, DbtSource
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

RESOURCE_TYPE_TO_CLASS_MAP = {
    DbtResourceType.MODEL: DbtModel,
    DbtResourceType.SOURCE: DbtSource,
}


def find_model_sql(project: Project, model: str) -> str | None:
    paths = list(project.dbt_directory.glob(os.path.join("models", "**", f"{model}.sql")))

    if paths:
        # TODO Raise exception if multiple matches
        return paths[0]
    else:
        return None


def list_resources(
    project_dir: Path | str, resource_types: Optional[List[DbtResourceType]] = None
) -> List[DbtModel | DbtSource]:
    valid_resource_types = RESOURCE_TYPE_TO_CLASS_MAP.keys()

    if resource_types is None:
        resource_types = valid_resource_types

    for resource_type in resource_types:
        if resource_type not in valid_resource_types:
            raise ValueError(f"'resource_types' must be any of: {", ".join(valid_resource_types)}")

    resource_dicts = []

    cmd = dbt_list_command(
        profiles_dir=DBT_PROFILES_DIR,
        project_dir=project_dir,
        resource_types=resource_types,
        output="json",
    )
    stdout = subprocess.check_output(cmd, text=True, cwd=project_dir)

    for line in stdout.splitlines():
        try:
            resource = json.loads(line)
        except json.decoder.JSONDecodeError:
            continue

        resource_dicts.append(resource)

    source_file_paths = pydash.uniq(
        [
            resource["original_file_path"]
            for resource in resource_dicts
            if resource["resource_type"] == DbtResourceType.SOURCE
        ]
    )

    if source_file_paths:
        source_files = {
            file_path: safe_load_file(os.path.join(project_dir, file_path))
            for file_path in source_file_paths
        }

        for resource in resource_dicts:
            if resource["resource_type"] == DbtResourceType.SOURCE:
                original_config = None
                data = source_files[resource["original_file_path"]]

                for source in data["sources"]:
                    if source["name"] == resource["source_name"]:
                        for table in source["tables"]:
                            if table["name"] == resource["name"]:
                                original_config = table
                                break
                    if original_config:
                        break

                resource["original_config"] = original_config

    resources = []
    for resource in resource_dicts:
        class_ = RESOURCE_TYPE_TO_CLASS_MAP[resource["resource_type"]]
        resources.append(class_(**resource))

    return resources


def dbt_list_command_args(
    fail_fast=True,
    use_colors=False,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    output: Optional[str] = None,
    resource_types: Optional[List[DbtResourceType]] = None,
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

    if resource_types:
        for resource_type in resource_types:
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
    resource_types: Optional[List[DbtResourceType]] = None,
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
            resource_types=resource_types,
            select=select,
            selector=selector,
            target=target,
            vars=vars,
        )
    )

    return cmd


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
