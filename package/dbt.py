from prefect_shell import ShellOperation
from typing import Any, Dict, List, Optional

import json


def dbt_run_command(
    profiles_dir: str,
    project_dir: str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[Dict[str, Any]] = None,
) -> List[str]:
    cmd = ["dbt", "run", "--no-use-colors", "--fail-fast"]
    cmd.extend(["--profiles-dir", profiles_dir])
    cmd.extend(["--project-dir", project_dir])

    if exclude:
        cmd.extend(["--exclude", exclude])

    if models:
        cmd.extend(["--models", models])

    if select:
        cmd.extend(["--select", select])

    if selector:
        cmd.extend(["--selector", selector])

    if target:
        cmd.extend(["--target", target])

    if vars:
        cmd.extend(["--vars", f"'{json.dumps(vars)}'"])

    return cmd


async def dbt_run(
    profiles_dir: str,
    project_dir: str,
    exclude: Optional[str] = None,
    models: Optional[str] = None,
    select: Optional[str] = None,
    selector: Optional[str] = None,
    target: Optional[str] = None,
    vars: Optional[Dict[str, Any]] = None,
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
