__all__ = ["Dbt"]

from dbt.cli.main import dbtRunnerResult
from package.utils.dbt_utils import dbt_list_sync, dbt_run_sync, dbt_seed_sync

import pydash


class Dbt:
    def __init__(self, project_dir: str) -> None:
        self.project_dir = project_dir
        self.target = "test"

    def list(self, **kwargs) -> dbtRunnerResult:
        return dbt_list_sync(self.project_dir, **pydash.defaults(kwargs, {"target": self.target}))

    def run(self, **kwargs) -> dbtRunnerResult:
        return dbt_run_sync(self.project_dir, **pydash.defaults(kwargs, {"target": self.target}))

    def seed(self, **kwargs) -> dbtRunnerResult:
        return dbt_seed_sync(self.project_dir, **pydash.defaults(kwargs, {"target": self.target}))
