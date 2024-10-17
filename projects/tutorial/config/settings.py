from dataclasses import dataclass, field
from functools import lru_cache
from package.database import build_connection_url
from package.project import Project

project = Project.from_path(__file__)


@dataclass
class ProjectSettings:
    DIRECTORY: str = field(default_factory=lambda: project.directory)


@dataclass
class DatabaseSettings:
    TYPE: str = field(default_factory=lambda: project.destination_database_settings["type"])
    DRIVER: str = field(default_factory=lambda: project.destination_database_settings["driver"])
    HOST: str = field(default_factory=lambda: project.destination_database_settings["host"])
    PORT: str = field(default_factory=lambda: project.destination_database_settings["port"])
    USERNAME: str = field(default_factory=lambda: project.destination_database_settings["username"])
    PASSWORD: str = field(default_factory=lambda: project.destination_database_settings["password"])
    DATABASE: str = field(default_factory=lambda: project.destination_database_settings["database"])
    URL: str = field(
        default_factory=lambda: build_connection_url(**project.destination_database_settings)
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
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    dbt: DbtSettings = field(default_factory=DbtSettings)
    notebook: NotebookSettings = field(default_factory=NotebookSettings)


@lru_cache(maxsize=1, typed=True)
def get_settings() -> Settings:
    return Settings()
