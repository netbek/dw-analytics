from package.constants import DBT_PROFILES_DIR  # noqa: F401
from package.project import Project

project = Project.from_path(__file__)

# Filesystem
PROJECT_DIR = project.directory
DBT_PROJECT_DIR = project.dbt_directory
NOTEBOOKS_DIR = project.notebooks_directory

# Database
CLICKHOUSE_URL = project.database_connection_url
