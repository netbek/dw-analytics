from package.cli.root import app
from package.config.constants import PEERDB_DESTINATION_PEER, PEERDB_SOURCE_PEER
from package.peerdb import DestinationPeer, PeerDBClient, SourcePeer
from package.project import Project
from package.types import PGTableIdentifier
from package.utils.typer_utils import typer_async

import typer

peerdb_app = typer.Typer(name="peerdb", add_completion=False)
app.add_typer(peerdb_app)


@peerdb_app.command()
@typer_async
async def install(project_name: str) -> None:
    project = Project.from_name(project_name)
    peerdb_config = project.settings.peerdb.processed_config
    peerdb_client = PeerDBClient(project.peerdb_api_url)
    source_peer = SourcePeer(project.settings.source_db)
    destination_peer = DestinationPeer(
        project.settings.destination_db,
        peerdb_config["peers"][PEERDB_DESTINATION_PEER]["clickhouse_config"]["database"],
    )

    if PEERDB_SOURCE_PEER in peerdb_config["users"]:
        source_peer.create_user(**peerdb_config["users"][PEERDB_SOURCE_PEER])
        app.console.print(
            f"Created user '{peerdb_config["users"][PEERDB_SOURCE_PEER]["username"]}' on source",
            style="green",
        )

        for schema in peerdb_config["publication_schemas"]:
            source_peer.grant_user_privileges(
                peerdb_config["users"][PEERDB_SOURCE_PEER]["username"], schema
            )
            app.console.print(
                f"Granted privileges to user '{peerdb_config["users"][PEERDB_SOURCE_PEER]["username"]}' on source schema '{schema}'",
                style="green",
            )

    for mirror in peerdb_config["mirrors"].values():
        for table_mapping in mirror["table_mappings"]:
            if "replica_identity" in table_mapping:
                table_identifier = PGTableIdentifier.from_string(
                    table_mapping["source_table_identifier"]
                )
                source_peer.set_table_replica_identity(
                    table_identifier.table,
                    table_mapping["replica_identity"],
                    database=table_identifier.database,
                    schema=table_identifier.schema_,
                )
                app.console.print(
                    f"Set replica identity of '{table_mapping["source_table_identifier"]}' to '{table_mapping["replica_identity"]}'",
                    style="green",
                )

    for publication in peerdb_config["publications"].values():
        source_peer.create_publication(
            publication["name"], publication["table_identifiers"], replace=True
        )
        app.console.print(
            f"Created publication '{publication["name"]}' on source",
            style="green",
        )

    destination_peer.create_database(destination_peer.database)
    app.console.print(
        f"Created database '{destination_peer.database}' on destination",
        style="green",
    )

    peerdb_client.update_settings(peerdb_config["settings"])
    app.console.print("Updated PeerDB settings", style="green")

    for peer in peerdb_config["peers"].values():
        peerdb_client.create_peer(peer)
        app.console.print(
            f"Created PeerDB peer '{peer['name']}'",
            style="green",
        )

    for mirror in peerdb_config["mirrors"].values():
        peerdb_client.create_mirror(mirror)
        app.console.print(
            f"Created PeerDB mirror '{mirror['flow_job_name']}'",
            style="green",
        )


@peerdb_app.command()
@typer_async
async def uninstall(project_name: str) -> None:
    project = Project.from_name(project_name)
    peerdb_config = project.settings.peerdb.processed_config
    peerdb_client = PeerDBClient(project.peerdb_api_url)
    source_peer = SourcePeer(project.settings.source_db)
    destination_peer = DestinationPeer(
        project.settings.destination_db,
        peerdb_config["peers"][PEERDB_DESTINATION_PEER]["clickhouse_config"]["database"],
    )

    for mirror in peerdb_config["mirrors"].values():
        peerdb_client.drop_mirror(mirror)
        app.console.print(
            f"Dropped PeerDB mirror '{mirror['flow_job_name']}'",
            style="green",
        )

    for peer in peerdb_config["peers"].values():
        peerdb_client.drop_peer(peer)
        app.console.print(
            f"Dropped PeerDB peer '{peer['name']}'",
            style="green",
        )

    for publication in peerdb_config["publications"].values():
        source_peer.drop_publication(publication["name"])
        app.console.print(
            f"Dropped publication '{publication["name"]}' on source",
            style="green",
        )

    for mirror in peerdb_config["mirrors"].values():
        for table_mapping in mirror["table_mappings"]:
            if "replica_identity" in table_mapping:
                table_identifier = PGTableIdentifier.from_string(
                    table_mapping["source_table_identifier"]
                )
                replica_identity = "default"
                source_peer.set_table_replica_identity(
                    table_identifier.table,
                    replica_identity,
                    database=table_identifier.database,
                    schema=table_identifier.schema_,
                )
                app.console.print(
                    f"Set replica identity of '{table_mapping["source_table_identifier"]}' to '{replica_identity}'",
                    style="green",
                )

    if PEERDB_SOURCE_PEER in peerdb_config["users"]:
        for schema in peerdb_config["publication_schemas"]:
            source_peer.revoke_user_privileges(
                peerdb_config["users"][PEERDB_SOURCE_PEER]["username"], schema
            )
            app.console.print(
                f"Revoked privileges from user '{peerdb_config["users"][PEERDB_SOURCE_PEER]["username"]}' on source schema '{schema}'",
                style="green",
            )

        source_peer.drop_user(peerdb_config["users"][PEERDB_SOURCE_PEER]["username"])
        app.console.print(
            f"Dropped user '{peerdb_config["users"][PEERDB_SOURCE_PEER]["username"]}' on source",
            style="green",
        )

    destination_peer.drop_database(destination_peer.database)
    app.console.print(
        f"Dropped database '{destination_peer.database}' on destination",
        style="green",
    )
