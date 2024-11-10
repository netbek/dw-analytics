from package.config.constants import DBT_PROFILES_DIR
from package.utils.dbt_utils import dbt_run
from prefect import flow
from projects.tutorial.config.settings import get_settings
from typing import Optional

import asyncio

settings = get_settings()


@flow(name="tutorial__dbt_run_flow")
async def dbt_run_flow(select: Optional[str] = None):
    return await dbt_run(
        profiles_dir=DBT_PROFILES_DIR, project_dir=settings.dbt.directory, select=select
    )


if __name__ == "__tutorial__":
    asyncio.run(dbt_run_flow())
