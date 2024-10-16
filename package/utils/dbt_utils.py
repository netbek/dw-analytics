from pathlib import Path
from prefect_shell.commands import ShellOperation
from typing import Any, Optional

import json


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
