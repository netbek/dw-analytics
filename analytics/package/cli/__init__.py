__all__ = ["app", "dbt_cli", "peerdb_cli", "prefect_cli", "project_cli"]

from package.cli.root import app

import package.cli.dbt_cli as dbt_cli
import package.cli.peerdb_cli as peerdb_cli
import package.cli.prefect_cli as prefect_cli
import package.cli.project_cli as project_cli
