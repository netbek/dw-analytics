from functools import lru_cache
from package.config.constants import HOME_DIR, PROJECTS_DIR, TEMPLATE_PROJECT_DIR
from package.utils.environ_utils import get_env_var
from package.utils.filesystem import rmtree, symlink
from package.utils.template import render_template
from pathlib import Path
from prefect import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterTags
from typing import Optional

import os
import prefect
import re
import shutil
import yaml


class Project:
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return f"Project '{self._name}'"

    @classmethod
    def from_name(cls, name):
        if not cls.exists(name):
            raise Exception(f"Project '{name}' not found")

        return cls(name)

    @classmethod
    def from_path(cls, path: str) -> Optional["Project"]:
        parent_dir = os.path.abspath(PROJECTS_DIR)
        child_dir = os.path.abspath(path)

        if os.path.isfile(child_dir):
            child_dir = os.path.dirname(child_dir)

        if parent_dir == child_dir or os.path.commonpath([parent_dir, child_dir]) != parent_dir:
            raise Exception(f"Path must be a subdirectory of {PROJECTS_DIR}")

        name = os.path.relpath(child_dir, parent_dir).split(os.path.sep)[0]

        return cls.from_name(name)

    @classmethod
    def exists(cls, name: str) -> bool:
        return bool([project for project in cls.list_projects() if project.name == name])

    @classmethod
    def list_projects(cls) -> ["Project"]:
        return [cls(name) for name in list(os.listdir(PROJECTS_DIR))]

    @classmethod
    def validate_name(cls, name: str) -> bool:
        if not isinstance(name, str):
            raise Exception("Name must be a string")

        if len(name.strip()) < 1:
            raise Exception("Name must be 1 or more characters long")

        if not re.match("^[a-z0-9_]+$", name):
            raise Exception("Name must contain only lowercase letters, numbers or underscore")

        if not name[0].isalpha():
            raise Exception("Name must start with a letter")

        if name == "default":
            raise Exception(f"Name must not be '{name}'")

        return True

    @property
    def name(self) -> str:
        return self._name

    @property
    @lru_cache
    def directory(self) -> Path:
        return Path(os.path.join(PROJECTS_DIR, self.name))

    @property
    @lru_cache
    def dbt_directory(self) -> Path:
        return Path(os.path.join(self.directory, "dbt"))

    @property
    @lru_cache
    def dbt_config_path(self) -> Path:
        return Path(os.path.join(self.directory, "dbt", "dbt_project.yml"))

    @property
    @lru_cache
    def dbt_docs_directory(self) -> Path:
        return Path(os.path.join(self.directory, "dbt", "docs"))

    @property
    @lru_cache
    def flows_directory(self) -> Path:
        return Path(os.path.join(self.directory, "flows"))

    @property
    @lru_cache
    def notebooks_directory(self) -> Path:
        return Path(os.path.join(self.directory, "notebooks"))

    @property
    @lru_cache
    def peerdb_api_url(self) -> str:
        return get_env_var("PEERDB_API_URL")

    @property
    @lru_cache
    def peerdb_config_path(self) -> Path:
        return Path(os.path.join(self.directory, "peerdb.yaml"))

    @property
    @lru_cache
    def prefect_config_path(self) -> Path:
        return Path(os.path.join(self.directory, "prefect.yaml"))

    @property
    @lru_cache
    def source_db_settings(self) -> dict:
        host = get_env_var("SOURCE_POSTGRES_HOST")
        port = get_env_var("SOURCE_POSTGRES_PORT")
        username = get_env_var("SOURCE_POSTGRES_USERNAME")
        password = get_env_var("SOURCE_POSTGRES_PASSWORD")
        database = get_env_var("SOURCE_POSTGRES_DATABASE")

        return {
            "type": "postgresql",
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database,
        }

    @property
    @lru_cache
    def destination_db_settings(self) -> dict:
        namespace = self.name.upper()
        driver = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_DRIVER")
        host = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_HOST")
        port = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_PORT")
        username = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_USERNAME")
        password = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_PASSWORD")
        database = get_env_var(f"{namespace}_DESTINATION_CLICKHOUSE_DATABASE")

        return {
            "type": "clickhouse",
            "driver": driver,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database,
        }

    def init_directory(self):
        """Copy template project files to the project directory."""
        rmtree(self.directory)

        shutil.copytree(TEMPLATE_PROJECT_DIR, self.directory)

        context = {"project_name": self.name, "prefect_version": prefect.__version__}
        render_template(os.path.join(self.directory, "README.md"), context)
        render_template(os.path.join(self.directory, "prefect.yaml"), context)
        render_template(os.path.join(self.dbt_directory, "dbt_project.yml"), context)

    def sync_dbt_directory(self):
        """Copy global files to the project dbt directory."""
        symlink(
            os.path.join(HOME_DIR, ".sqlfluffignore"),
            os.path.join(self.dbt_directory, ".sqlfluffignore"),
        )

        rmtree(os.path.join(self.dbt_directory, "macros", "dbt"))
        shutil.copytree(
            os.path.join(TEMPLATE_PROJECT_DIR, "dbt", "macros", "dbt"),
            os.path.join(self.dbt_directory, "macros", "dbt"),
        )

    def load_dbt_config(self) -> dict:
        with open(self.dbt_config_path, "rt") as fp:
            config = yaml.safe_load(fp)

        return config

    def load_prefect_config(self) -> dict:
        with open(self.prefect_config_path, "rt") as fp:
            config = yaml.safe_load(fp)

        return config

    async def create(self):
        self.init_directory()
        self.sync_dbt_directory()

    async def refresh(self):
        self.sync_dbt_directory()

    async def delete(self):
        from package.cli.prefect_cli import delete_deployment

        async with get_client() as client:
            filter = DeploymentFilter(tags=DeploymentFilterTags(any_=[self.name]))
            deployments = await client.read_deployments(deployment_filter=filter)

            for deployment in deployments:
                await delete_deployment(client, deployment.id)

        rmtree(self.directory)
