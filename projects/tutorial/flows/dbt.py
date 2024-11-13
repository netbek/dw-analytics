from package.dbt_utils import Dbt
from prefect import flow
from projects.tutorial.config.settings import get_settings
from typing import Optional

import asyncio

settings = get_settings()


@flow(name="tutorial__dbt_run_flow")
async def dbt_run_flow(select: Optional[str] = None):
    return await Dbt(settings.dbt.directory).run_async(select=select)


if __name__ == "__tutorial__":
    asyncio.run(dbt_run_flow())
