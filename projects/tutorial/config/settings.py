from functools import lru_cache
from package.project import Project
from package.types import CHSettings, PGSettings
from package.utils.settings import create_ch_settings, create_pg_settings
from package.utils.yaml_utils import safe_load_file
from pydantic import BaseModel, Field

project = Project.from_path(__file__)


class Settings(BaseModel):
    source_db: PGSettings = Field(default_factory=create_pg_settings("source_"))
    destination_db: CHSettings = Field(
        default_factory=create_ch_settings(f"{project.name}_destination_")
    )
    dbt: dict = Field(default_factory=lambda: safe_load_file(project.dbt_config_path))
    prefect: dict = Field(default_factory=lambda: safe_load_file(project.prefect_config_path))


@lru_cache(maxsize=1, typed=True)
def get_settings() -> Settings:
    return Settings()
