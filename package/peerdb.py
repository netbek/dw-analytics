from package.database import CHAdapter, PGAdapter
from package.types import CHSettings, PGSettings, PGTableIdentifier
from package.utils.yaml_utils import safe_load_file
from pathlib import Path

import copy
import httpx
import pydash


def load_config_file(path: Path | str) -> dict:
    config = safe_load_file(path)
    config = process_config(config)

    return config


def process_node(node: dict) -> dict:
    default_keys = [key for key in node.keys() if key.startswith("+")]
    defaults = {key.lstrip("+").strip(): node[key] for key in default_keys}

    if defaults:
        for key in node.keys():
            if not key.startswith("+"):
                node[key] = pydash.defaults(node[key], defaults)

        node = pydash.omit(node, *default_keys)

    return node


def process_config(config: dict) -> dict:
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

        source_peer = result["peers"].get("source")

        if result["mirrors"] and source_peer:
            # TODO Add function that creates instance of PGSettings from PeerDB config
            pg_settings = PGSettings(
                host=source_peer["postgres_config"]["host"],
                port=source_peer["postgres_config"]["port"],
                username=source_peer["postgres_config"]["user"],
                password=source_peer["postgres_config"]["password"],
                database=source_peer["postgres_config"]["database"],
                schema_="public",
            )
            db = PGAdapter(pg_settings)

            for mirror in result["mirrors"].values():
                computed_table_mappings = []

                for table_mapping in mirror["table_mappings"]:
                    table_identifier = PGTableIdentifier.from_string(
                        table_mapping["source_table_identifier"]
                    )
                    table = db.get_table(**table_identifier.model_dump(by_alias=True))

                    if table is None:
                        raise Exception(
                            f"Source table '{table_mapping["source_table_identifier"]}' not found"
                        )

                    include = table_mapping.get("include", None)
                    exclude = table_mapping.get("exclude", None)
                    computed_exclude = []

                    if include is not None and exclude is not None:
                        raise Exception(
                            "Table mapping may contain either 'include' or 'exclude' value, not both"
                        )
                    elif include is not None:
                        columns = [column.name for column in table.columns]
                        computed_exclude = pydash.difference(columns, include)
                    elif exclude is not None:
                        columns = [column.name for column in table.columns]
                        computed_exclude = pydash.intersection(columns, exclude)
                    else:
                        computed_exclude = []

                    computed_exclude = sorted(computed_exclude)
                    computed_table_mappings.append(
                        {
                            "source_table_identifier": table_mapping["source_table_identifier"],
                            "destination_table_identifier": table_mapping[
                                "destination_table_identifier"
                            ],
                            "exclude": computed_exclude,
                        }
                    )

                mirror["table_mappings"] = computed_table_mappings
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

    result["publication_schemas"] = sorted(pydash.uniq(publication_schemas))

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

    def has_peer(self, peer: dict) -> bool:
        url = f"{self._api_url}/v1/peers/list"
        response = httpx.get(url, headers=self._headers)
        matches = [item for item in response.json()["items"] if item["name"] == peer["name"]]

        return bool(matches)

    def create_peer(self, peer: dict) -> None:
        if not self.has_peer(peer):
            url = f"{self._api_url}/v1/peers/create"
            data = {"peer": peer}
            response = httpx.post(url, json=data, headers=self._headers)

            if not (response.status_code == 200 and response.json()["status"] == "CREATED"):
                raise Exception(
                    f"Failed to create peer '{peer['name']}' (error {response.status_code}: {response.text})"
                )

    def drop_peer(self, peer: dict) -> None:
        if self.has_peer(peer):
            url = f"{self._api_url}/v1/peers/drop"
            data = {"peerName": peer["name"]}
            response = httpx.post(url, json=data, headers=self._headers, timeout=None)

            if not (response.status_code == 200 and response.json()["ok"]):
                raise Exception(
                    f"Failed to drop peer '{peer['name']}' (error {response.status_code}: {response.text})"
                )

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
            response = httpx.post(url, json=data, headers=self._headers, timeout=None)

            if not (response.status_code == 200 and response.json()["ok"]):
                raise Exception(
                    f"Failed to drop mirror '{mirror['flow_job_name']}' (error {response.status_code}: {response.text})"
                )


class SourcePeer(PGAdapter):
    def create_user(self, username: str, password: str) -> None:
        return super().create_user(username, password, options={"login": True, "replication": True})


class DestinationPeer(CHAdapter):
    def __init__(self, db_settings: CHSettings, database: str) -> None:
        self._database = database
        super().__init__(db_settings)

    @property
    def database(self) -> str:
        return self._database
