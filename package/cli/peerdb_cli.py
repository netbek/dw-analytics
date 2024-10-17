from package.cli.root import app
from package.peerdb import DestinationPeer, PeerDBClient, PeerDBConfig, SourcePeer
from package.project import Project
from package.utils.typer_utils import typer_async

import typer

peerdb_app = typer.Typer(name="peerdb", add_completion=False)
app.add_typer(peerdb_app)


@peerdb_app.command()
@typer_async
async def install(project_name: str):
    project = Project.from_name(project_name)
    peerdb_config = PeerDBConfig(project.peerdb_config_path).load()
    peerdb_client = PeerDBClient(project.peerdb_api_url)
    source_peer = SourcePeer(project.source_db_settings)
    destination_peer = DestinationPeer(
        project.destination_db_settings,
        peerdb_config["peers"]["destination"]["clickhouse_config"]["database"],
    )

    if "source" in peerdb_config["users"]:
        source_peer.create_user(**peerdb_config["users"]["source"])
        app.console.print(
            f"Created user '{peerdb_config["users"]["source"]["username"]}' on source",
            style="green",
        )

        for schema in peerdb_config["publication_schemas"]:
            source_peer.grant_privileges(peerdb_config["users"]["source"]["username"], schema)
            app.console.print(
                f"Granted privileges to user '{peerdb_config["users"]["source"]["username"]}' on source schema '{schema}'",
                style="green",
            )

    for publication in peerdb_config["publications"].values():
        source_peer.create_publication(publication["name"], publication["table_identifiers"])
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
async def uninstall(project_name: str):
    project = Project.from_name(project_name)
    peerdb_config = PeerDBConfig(project.peerdb_config_path).load()
    peerdb_client = PeerDBClient(project.peerdb_api_url)
    source_peer = SourcePeer(project.source_db_settings)
    destination_peer = DestinationPeer(
        project.destination_db_settings,
        peerdb_config["peers"]["destination"]["clickhouse_config"]["database"],
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

    if "source" in peerdb_config["users"]:
        for schema in peerdb_config["publication_schemas"]:
            source_peer.revoke_privileges(peerdb_config["users"]["source"]["username"], schema)
            app.console.print(
                f"Revoked privileges from user '{peerdb_config["users"]["source"]["username"]}' on source schema '{schema}'",
                style="green",
            )

        source_peer.drop_user(peerdb_config["users"]["source"]["username"])
        app.console.print(
            f"Dropped user '{peerdb_config["users"]["source"]["username"]}' on source",
            style="green",
        )

    destination_peer.drop_database(destination_peer.database)
    app.console.print(
        f"Dropped database '{destination_peer.database}' on destination",
        style="green",
    )
