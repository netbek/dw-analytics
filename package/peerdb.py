from functools import lru_cache
from package.database import build_connection_url, get_clickhouse_client, get_postgres_client
from pathlib import Path
from typing import List

import copy
import httpx
import pydash
import yaml


class PeerDBConfig:
    def __init__(self, path: Path) -> None:
        self._path = path

    @lru_cache
    def load(self):
        with open(self._path, "rt") as fp:
            config = yaml.safe_load(fp)
            config = self.process(config)

        return config

    @classmethod
    def process(cls, config: dict) -> dict:
        def process_node(node: dict) -> dict:
            default_keys = [key for key in node.keys() if key.startswith("+")]
            defaults = {key.lstrip("+").strip(): node[key] for key in default_keys}

            if defaults:
                for key in node.keys():
                    if not key.startswith("+"):
                        node[key] = pydash.defaults(node[key], defaults)

                node = pydash.omit(node, *default_keys)

            return node

        result = copy.deepcopy(config)

        if "users" not in result:
            result["users"] = {}

        if "publications" in result:
            for key, value in result["publications"].items():
                result["publications"][key] = {
                    "name": key,
                    "table_identifiers": value,
                }
        else:
            result["publications"] = {}

        if "peers" in result:
            result["peers"] = process_node(result["peers"])

            for key, value in result["peers"].items():
                result["peers"][key]["name"] = key
        else:
            result["peers"] = {}

        if "mirrors" in result:
            result["mirrors"] = process_node(result["mirrors"])

            for key, value in result["mirrors"].items():
                result["mirrors"][key]["flow_job_name"] = key
        else:
            result["mirrors"] = {}

        publication_schemas = []

        for value in result["publications"].values():
            for identifier in value["table_identifiers"]:
                parts = identifier.split(".")
                if len(parts) == 2:
                    publication_schemas.append(parts[0])

        for value in result["mirrors"].values():
            for table_mapping in value["table_mappings"]:
                parts = table_mapping["source_table_identifier"].split(".")
                if len(parts) == 2:
                    publication_schemas.append(parts[0])

        result["publication_schemas"] = pydash.uniq(publication_schemas)

        return result


class PeerDBClient:
    def __init__(self, api_url: str) -> None:
        self._api_url = api_url
        self._headers = {"Content-Type": "application/json"}

    def update_settings(self, settings: dict) -> None:
        url = f"{self._api_url}/v1/dynamic_settings"

        for key, value in settings.items():
            data = {"name": key, "value": value}
            response = httpx.post(url, json=data, headers=self._headers)

            if response.status_code != 200:
                raise Exception(
                    f"Failed to set {key}={value} (error {response.status_code}: {response.text})"
                )

    def create_peer(self, peer: dict) -> None:
        url = f"{self._api_url}/v1/peers/create"
        data = {"peer": peer}
        response = httpx.post(url, json=data, headers=self._headers)

        if not (response.status_code == 200 and response.json()["status"] == "CREATED"):
            raise Exception(
                f"Failed to create peer '{peer['name']}' (error {response.status_code}: {response.text})"
            )

    def drop_peer(self, peer: dict) -> None:
        raise NotImplementedError()

    def has_mirror(self, mirror: dict) -> bool:
        return self.get_mirror_status(mirror) != "STATUS_UNKNOWN"

    def get_mirror_status(self, mirror: dict) -> None:
        url = f"{self._api_url}/v1/mirrors/status"
        data = {"flowJobName": mirror["flow_job_name"]}
        response = httpx.post(url, json=data, headers=self._headers)

        return response.json()["currentFlowState"]

    def create_mirror(self, mirror: dict) -> None:
        if not self.has_mirror(mirror):
            url = f"{self._api_url}/v1/flows/cdc/create"
            data = {"connection_configs": mirror}
            response = httpx.post(url, json=data, headers=self._headers)

            if not (response.status_code == 200 and "workflowId" in response.json()):
                raise Exception(
                    f"Failed to create mirror '{mirror['flow_job_name']}' (error {response.status_code}: {response.text})"
                )

    def drop_mirror(self, mirror: dict) -> None:
        if self.has_mirror(mirror):
            url = f"{self._api_url}/v1/mirrors/state_change"
            data = {
                "flowJobName": mirror["flow_job_name"],
                "requestedFlowState": "STATUS_TERMINATED",
            }
            response = httpx.post(url, json=data, headers=self._headers)

            if not (response.status_code == 200 and response.json()["ok"]):
                raise Exception(
                    f"Failed to drop mirror '{mirror['flow_job_name']}' (error {response.status_code}: {response.text})"
                )


class Peer:
    def __init__(self, db_settings: dict) -> None:
        self._db_settings = db_settings
        self._db_url = build_connection_url(**db_settings)

    @property
    def database(self) -> str:
        return self._db_settings["database"]


class SourcePeer(Peer):
    def has_publication(self, publication_name: str) -> None:
        with get_postgres_client(self._db_url) as (conn, cur):
            cur.execute("select 1 from pg_publication where pubname = %s;", [publication_name])
            return bool(cur.fetchall())

    def create_publication(self, publication_name: str, table_identifiers: List[str]) -> None:
        if self.has_publication(publication_name):
            self.drop_publication(publication_name)

        with get_postgres_client(self._db_url) as (conn, cur):
            cur.execute(
                f"create publication {publication_name} for table {", ".join(table_identifiers)};"
            )

    def drop_publication(self, publication_name: str) -> None:
        if self.has_publication(publication_name):
            with get_postgres_client(self._db_url) as (conn, cur):
                cur.execute(f"drop publication {publication_name};")

    def has_user(self, username: str) -> None:
        with get_postgres_client(self._db_url) as (conn, cur):
            cur.execute("select 1 from pg_roles where rolname = %s;", [username])
            return bool(cur.fetchall())

    def create_user(self, username: str, password: str) -> None:
        if not self.has_user(username):
            with get_postgres_client(self._db_url) as (conn, cur):
                cur.execute(
                    f"""
                    create role {username} with replication login password %(password)s;
                    """,
                    {"password": password},
                )

    def drop_user(self, username: str) -> None:
        if self.has_user(username):
            with get_postgres_client(self._db_url) as (conn, cur):
                cur.execute(
                    f"""
                    drop owned by {username} cascade;
                    drop role {username};
                    """
                )

    def grant_privileges(self, username: str, schema: str) -> None:
        with get_postgres_client(self._db_url) as (conn, cur):
            cur.execute(
                f"""
                grant usage on schema {schema} to {username};
                grant select on all tables in schema {schema} to {username};
                alter default privileges in schema {schema} grant select on tables to {username};
                """,
            )

    def revoke_privileges(self, username: str, schema: str) -> None:
        if self.has_user(username):
            with get_postgres_client(self._db_url) as (conn, cur):
                cur.execute(
                    f"""
                    alter default privileges for role {username} in schema {schema} revoke select on tables from {username};
                    revoke select on all tables in schema {schema} from {username};
                    revoke usage on schema {schema} from {username};
                    -- reassign owned by {username} to postgres;
                    """
                )


class DestinationPeer(Peer):
    def has_database(self, db_name: str) -> bool:
        with get_clickhouse_client(self._db_url) as client:
            result = client.query(
                "select 1 from system.databases where name = {db_name:String};",
                parameters={"db_name": db_name},
            )
            return bool(result.result_rows)

    def create_database(self, db_name: str) -> None:
        if not self.has_database(db_name):
            with get_clickhouse_client(self._db_url) as client:
                client.command(
                    "create database {db_name:Identifier};",
                    parameters={"db_name": db_name},
                )

    def drop_database(self, db_name: str) -> None:
        if self.has_database(db_name):
            with get_clickhouse_client(self._db_url) as client:
                client.command(
                    "drop database {db_name:Identifier};",
                    parameters={"db_name": db_name},
                )

    def create_user(self, username: str, password: str):
        with get_clickhouse_client(self._db_url) as client:
            client.command(
                "create user {username:Identifier} identified by {password:String};",
                parameters={"username": username, "password": password},
            )

    def drop_user(self, username: str):
        with get_clickhouse_client(self._db_url) as client:
            client.command(
                "drop user {username:Identifier};",
                parameters={"username": username},
            )
