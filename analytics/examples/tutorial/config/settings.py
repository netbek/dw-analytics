from functools import lru_cache
from package.project import Project
from package.types import CHSettings, DbtSettings, NotebookSettings, PGSettings, PrefectSettings
from package.utils.settings import (
    create_ch_settings,
    create_dbt_settings,
    create_notebook_settings,
    create_pg_settings,
    create_prefect_settings,
)
from pydantic import BaseModel, Field

# from package.types import PeerDBSettings
# from package.utils.settings import create_peerdb_settings

project = Project.from_path(__file__)


class Settings(BaseModel):
    source_db: PGSettings = Field(default_factory=create_pg_settings("source_postgres_"))
    destination_db: CHSettings = Field(
        default_factory=create_ch_settings(f"{project.name}_destination_")
    )
    dbt: DbtSettings = Field(
        default_factory=create_dbt_settings(project.dbt_directory, project.dbt_config_path)
    )
    notebook: NotebookSettings = Field(
        default_factory=create_notebook_settings(project.notebooks_directory)
    )
    # peerdb: PeerDBSettings = Field(
    #     default_factory=create_peerdb_settings(project.peerdb_config_path)
    # )
    prefect: PrefectSettings = Field(
        default_factory=create_prefect_settings(project.prefect_config_path)
    )


@lru_cache(maxsize=1, typed=True)
def get_settings() -> Settings:
    return Settings()
