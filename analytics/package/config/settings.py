from functools import lru_cache
from package.types import CHSettings, PGSettings
from package.utils.settings import create_ch_settings, create_pg_settings
from pydantic import BaseModel, Field


class Settings(BaseModel):
    test_clickhouse: CHSettings = Field(
        default_factory=create_ch_settings("package_test_clickhouse_"),
    )
    test_postgres: PGSettings = Field(
        default_factory=create_pg_settings("package_test_postgres_"),
    )


@lru_cache(maxsize=1, typed=True)
def get_settings() -> Settings:
    return Settings()
