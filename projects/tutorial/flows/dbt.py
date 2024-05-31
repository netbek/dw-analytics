from package.utils.dbt_utils import dbt_run
from prefect import flow
from projects.tutorial.config.constants import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from typing import Optional

import asyncio


@flow(name="tutorial__dbt_run_flow")
async def dbt_run_flow(select: Optional[str] = None):
    return await dbt_run(profiles_dir=DBT_PROFILES_DIR, project_dir=DBT_PROJECT_DIR, select=select)


if __name__ == "__main__":
    asyncio.run(dbt_run_flow())
