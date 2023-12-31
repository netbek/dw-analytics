from package.cli.root import app
from package.constants import (
    ACTION_CREATE,
    ACTION_DELETE,
    ACTION_PAUSE,
    ACTION_RESUME,
    DeploymentAction,
    PREFECT_PROFILES_PATH,
    PREFECT_PROVISION_PATH,
)
from package.utils.project import get_project_dir, list_projects
from package.utils.typer_utils import typer_async
from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
from starlette import status
from typing import List, Optional
from uuid import UUID

import httpx
import os
import prefect
import subprocess
import typer
import yaml

prefect_app = typer.Typer(name="prefect", add_completion=False)
app.add_typer(prefect_app)


class DeploymentNotFoundError(Exception):
    def __init__(self, message="Deployment not found"):
        self.message = message
        super().__init__(self.message)


class ProfileNotFoundError(Exception):
    def __init__(self, message="Profile not found in config"):
        self.message = message
        super().__init__(self.message)


@prefect_app.command()
def version():
    print(prefect.__version__)


@prefect_app.command()
@typer_async
async def provision(
    profile_name: Optional[str] = typer.Argument(
        None, help="Profile name. Default is the active profile."
    )
):
    profiles = prefect.settings.load_profiles()
    current_profile = prefect.context.get_settings_context().profile
    current_name = current_profile.name if current_profile is not None else None

    with open(PREFECT_PROVISION_PATH, "rt") as file:
        provision_profiles = yaml.safe_load(file).get("profiles", {})

    if profile_name:
        if profile_name not in profiles:
            raise ProfileNotFoundError(
                f"Profile '{profile_name}' not found in {PREFECT_PROFILES_PATH}"
            )

        if profile_name not in provision_profiles:
            raise ProfileNotFoundError(
                f"Profile '{profile_name}' not found in {PREFECT_PROVISION_PATH}"
            )
    else:
        if current_name:
            profile_name = current_name
        else:
            raise Exception("Specify a profile because none are active")

    work_pools = provision_profiles[profile_name].get("work_pools", {})
    work_pools = [{"name": k, **v} for k, v in work_pools.items()]
    names = [work_pool["name"] for work_pool in work_pools]

    async with get_client() as client:
        existing = await client.read_work_pools()
        existing_names = [work_pool.name for work_pool in existing]

        # Delete existing work pools that aren't in configuration
        delete_names = [name for name in existing_names if name not in names]

        if delete_names:
            for name in delete_names:
                await client.delete_work_pool(name)
                app.console.print(f"Deleted work pool '{name}'", style="green")

        # Create or update work pools
        for work_pool in work_pools:
            if work_pool["name"] in existing_names:
                await client.update_work_pool(
                    work_pool["name"],
                    WorkPoolUpdate(concurrency_limit=work_pool["concurrency_limit"]),
                )
                app.console.print(f"Updated work pool '{work_pool['name']}'", style="green")
            else:
                await client.create_work_pool(WorkPoolCreate(**work_pool))
                app.console.print(f"Created work pool '{work_pool['name']}'", style="green")


@prefect_app.command()
@typer_async
async def deploy(
    project_names: Optional[List[str]] = typer.Option(
        None, "-p", "--project-name", help="1 or more project names"
    ),
    deployment_names: Optional[List[str]] = typer.Option(
        None, "-d", "--deployment-name", help="1 or more deployment names"
    ),
    pause: Optional[bool] = typer.Option(
        False,
        "--pause",
        help="Whether to create a paused deployment. By default, new deployments are active.",
    ),
):
    if not project_names and not deployment_names:
        raise Exception("1 or more project names and/or deployment names is required.")

    deploy_configs = _load_deploy_configs(
        project_names=project_names, deployment_names=deployment_names
    )
    selected_names = [deployment["name"] for deployment in deploy_configs]

    if not selected_names:
        raise Exception(
            "No deployments match the given project names and/or deployment names. "
            "Check the prefect.yaml configs."
        )

    async with get_client() as client:
        existing = await client.read_deployments()
        existing_names = [d.name for d in existing]

        try:
            deployment_actions = _build_deployment_actions(
                ACTION_CREATE, selected_names, existing_names
            )
        except Exception as e:
            app.console.print(e.message, style="red")
            return

        create_names = [d["name"] for d in deployment_actions if d["action"] == ACTION_CREATE]

        for name in create_names:
            create_deployments = [
                deployment for deployment in deploy_configs if deployment["name"] == name
            ]

            for deployment in create_deployments:
                cmd = [
                    "prefect",
                    "--no-prompt",
                    "deploy",
                    "--name",
                    deployment["name"],
                ]
                cwd = os.path.dirname(deployment["config_path"])
                subprocess.check_output(cmd, cwd=cwd)

            deployments = await client.read_deployments()
            deployments = [d for d in deployments if d.name == name]

            for deployment in deployments:
                app.console.print(f"Created deployment '{deployment.name}'", style="green")

                if pause:
                    await client.update_schedule(deployment.id, False)
                    app.console.print(f"Paused deployment '{deployment.name}'", style="green")

        update_names = [d["name"] for d in deployment_actions if d["action"] == ACTION_RESUME]
        update_deployments = [d for d in existing if d.name in update_names]

        for deployment in update_deployments:
            if pause:
                await client.update_schedule(deployment.id, False)
                app.console.print(f"Paused deployment '{deployment.name}'", style="green")
            else:
                await client.update_schedule(deployment.id, True)
                app.console.print(f"Resumed deployment '{deployment.name}'", style="green")


@prefect_app.command()
@typer_async
async def deployment(
    action: DeploymentAction,
    project_names: Optional[List[str]] = typer.Option(
        None, "-p", "--project-name", help="1 or more project names"
    ),
    deployment_names: Optional[List[str]] = typer.Option(
        None, "-d", "--deployment-name", help="1 or more deployment names"
    ),
):
    if not project_names and not deployment_names:
        raise Exception("1 or more project names and/or deployment names is required.")

    deploy_configs = _load_deploy_configs(
        project_names=project_names, deployment_names=deployment_names
    )
    selected_names = [deployment["name"] for deployment in deploy_configs]

    if not selected_names:
        raise Exception(
            "No deployments match the given project names and/or deployment names. "
            "Check the prefect.yaml configs."
        )

    async with get_client() as client:
        existing = await client.read_deployments()
        existing_names = [d.name for d in existing]

        try:
            deployment_actions = _build_deployment_actions(action, selected_names, existing_names)
        except Exception as e:
            app.console.print(e.message, style="red")
            return

        deployment_names = [d["name"] for d in deployment_actions if d["action"] == action]
        deployments = [d for d in existing if d.name in deployment_names]

        for deployment in deployments:
            if action == ACTION_DELETE:
                await delete_deployment(client, deployment.id)
            elif action == ACTION_PAUSE:
                await pause_deployment(client, deployment.id)
            elif action == ACTION_RESUME:
                await resume_deployment(client, deployment.id)


async def delete_deployment(client: PrefectClient, deployment_id: UUID):
    deployment = await client.read_deployment(deployment_id)

    await client.delete_deployment(deployment.id)
    app.console.print(f"Deleted deployment '{deployment.name}'", style="green")

    flows = await client.read_flows()
    flows = [flow for flow in flows if flow.id == deployment.flow_id]

    for flow in flows:
        # Get a list of deployments of the flow other than the given deployment
        other_deployments = await client.read_deployments()
        other_deployments = [
            d for d in other_deployments if d.flow_id == flow.id and d.id != deployment.id
        ]

        if not other_deployments:
            await delete_flow(client, flow.id)
            app.console.print(f"Deleted flow '{flow.name}'", style="green")


async def pause_deployment(client: PrefectClient, deployment_id: UUID):
    deployment = await client.read_deployment(deployment_id)

    await client.update_schedule(deployment.id, False)
    app.console.print(f"Paused deployment '{deployment.name}'", style="green")


async def resume_deployment(client: PrefectClient, deployment_id: UUID):
    deployment = await client.read_deployment(deployment_id)

    await client.update_schedule(deployment.id, True)
    app.console.print(f"Resumed deployment '{deployment.name}'", style="green")


# TODO Remove after delete_flow() has been added to PrefectClient
async def delete_flow(client: PrefectClient, flow_id: UUID):
    try:
        await client._client.delete(f"/flows/{flow_id}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == status.HTTP_404_NOT_FOUND:
            raise prefect.exceptions.ObjectNotFound(http_exc=e) from e
        else:
            raise


def _load_deploy_configs(
    project_names: Optional[List[str]] = None, deployment_names: Optional[List[str]] = None
):
    deploy_configs = []
    all_project_names = list_projects()

    if project_names:
        selected_project_names = [name for name in all_project_names if name in project_names]
    else:
        selected_project_names = all_project_names

    for project_name in selected_project_names:
        project_dir = get_project_dir(project_name)
        config_path = os.path.join(project_dir, "prefect.yaml")

        with open(config_path) as file:
            config = yaml.safe_load(file)

        deployments = config.get("deployments") or {}

        for deployment in deployments:
            if deployment_names and deployment["name"] not in deployment_names:
                continue

            deploy_configs.append(
                {
                    **deployment,
                    "project_name": project_name,
                    "config_path": config_path,
                }
            )

    return deploy_configs


def _build_deployment_actions(
    action: str, selected_names: [str], existing_names: Optional[List[str]] = None
):
    result = []

    if not existing_names:
        existing_names = []

    if action == ACTION_CREATE:
        for name in selected_names:
            if name in existing_names:
                result.append({"action": ACTION_RESUME, "name": name})
            else:
                result.append({"action": ACTION_CREATE, "name": name})
    else:
        not_found = []

        for name in selected_names:
            if name in existing_names:
                result.append({"action": action, "name": name})
            else:
                not_found.append(name)

        if not_found:
            quoted = list(map(lambda x: f"'{x}'", not_found))
            raise DeploymentNotFoundError(f"Deployment {', '.join(quoted)} not found")

    return result
