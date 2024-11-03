from dataclasses import dataclass, field
from functools import lru_cache
from package.database import create_connection_url
from package.project import Project

project = Project.from_path(__file__)


@dataclass
class ProjectSettings:
    DIRECTORY: str = field(default_factory=lambda: project.directory)


@dataclass
class DatabaseSettings:
    URL: str = field(
        default_factory=lambda: create_connection_url(**project.destination_db_settings)
    )


@dataclass
class DbtSettings:
    DIRECTORY: str = field(default_factory=lambda: project.dbt_directory)


@dataclass
class NotebookSettings:
    DIRECTORY: str = field(default_factory=lambda: project.notebooks_directory)


@dataclass
class Settings:
    project: ProjectSettings = field(default_factory=ProjectSettings)
    db: DatabaseSettings = field(default_factory=DatabaseSettings)
    dbt: DbtSettings = field(default_factory=DbtSettings)
    notebook: NotebookSettings = field(default_factory=NotebookSettings)


@lru_cache(maxsize=1, typed=True)
def get_settings() -> Settings:
    return Settings()
