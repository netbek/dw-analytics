from package.cli.prefect_cli import delete_deployment
from package.cli.root import app
from package.constants import DBT_PROFILES_FILE, HOME_DIR, TEMPLATE_PROJECT_DIR
from package.utils.filesystem import rmtree, symlink
from package.utils.project import get_project_dir, project_exists
from package.utils.template import render_template
from package.utils.typer_utils import typer_async
from prefect import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterTags

import os
import prefect
import shutil
import subprocess
import typer

project_app = typer.Typer(name="project", add_completion=False)
app.add_typer(project_app)


@project_app.command()
def init(project_name: str):
    if project_exists(project_name):
        raise Exception(f"Project '{project_name}' already exists")

    copy_template_project(project_name)
    sync_project_dbt(project_name)

    app.console.print("To complete the setup, perform these steps:")
    app.console.print(f"1. Add a profile for '{project_name}' to {DBT_PROFILES_FILE}")
    app.console.print(f"2. Run 'cli project refresh {project_name}'")


@project_app.command()
@typer_async
async def delete(project_name: str):
    if not project_exists(project_name):
        raise Exception(f"Project '{project_name}' not found")

    project_dir = get_project_dir(project_name)

    confirmed = typer.confirm("Are you sure you want to delete the project's files and data?")
    if not confirmed:
        raise typer.Abort()

    async with get_client() as client:
        filter = DeploymentFilter(tags=DeploymentFilterTags(any_=[project_name]))
        deployments = await client.read_deployments(deployment_filter=filter)

        for deployment in deployments:
            await delete_deployment(client, deployment.id)

    rmtree(project_dir)

    app.console.print("To complete the deletion, perform these steps:")
    app.console.print(f"1. Remove the profile for '{project_name}' from {DBT_PROFILES_FILE}")


@project_app.command()
def refresh(project_name: str):
    if not project_exists(project_name):
        raise Exception(f"Project '{project_name}' not found")

    project_dir = get_project_dir(project_name)

    sync_project_dbt(project_name)

    subprocess.run("dbt clean", shell=True, cwd=os.path.join(project_dir, "dbt"))
    subprocess.run("dbt deps", shell=True, cwd=os.path.join(project_dir, "dbt"))
    subprocess.run("dbt seed", shell=True, cwd=os.path.join(project_dir, "dbt"))


@project_app.command()
def lint(project_name: str):
    if not project_exists(project_name):
        raise Exception(f"Project '{project_name}' not found")

    project_dir = get_project_dir(project_name)
    yamllint_config_file = os.path.join(HOME_DIR, ".yamllint")
    sqlfluff_config_file = os.path.join(HOME_DIR, ".sqlfluff")

    subprocess.run(
        f"yamllint -c {yamllint_config_file} .", shell=True, cwd=os.path.join(project_dir, "dbt")
    )
    subprocess.run(
        f"sqlfluff lint --config {sqlfluff_config_file} --ignore-local-config --ignore parsing .",
        shell=True,
        cwd=os.path.join(project_dir, "dbt"),
    )


@project_app.command()
def fix(project_name: str):
    if not project_exists(project_name):
        raise Exception(f"Project '{project_name}' not found")

    project_dir = get_project_dir(project_name)
    sqlfluff_config_file = os.path.join(HOME_DIR, ".sqlfluff")

    subprocess.run(
        f"sqlfluff fix --config {sqlfluff_config_file} --ignore-local-config --ignore parsing .",
        shell=True,
        cwd=os.path.join(project_dir, "dbt"),
    )


@project_app.command()
def test(project_name: str):
    if not project_exists(project_name):
        raise Exception(f"Project '{project_name}' not found")

    project_dir = get_project_dir(project_name)

    subprocess.run("dbt test -t test -q", shell=True, cwd=os.path.join(project_dir, "dbt"))
    subprocess.run("pytest", shell=True, cwd=os.path.join(project_dir, "tests"))


def copy_template_project(project_name: str):
    """Copy template project files to the given project."""
    project_dir = get_project_dir(project_name)

    rmtree(project_dir)

    shutil.copytree(TEMPLATE_PROJECT_DIR, project_dir)

    context = {"project_name": project_name, "prefect_version": prefect.__version__}
    render_template(os.path.join(project_dir, "README.md"), context)
    render_template(os.path.join(project_dir, "prefect.yaml"), context)
    render_template(os.path.join(project_dir, "dbt", "dbt_project.yml"), context)


def sync_project_dbt(project_name: str):
    """Copy global files to the given project's dbt directory."""
    project_dir = get_project_dir(project_name)

    symlink(
        os.path.join(HOME_DIR, ".sqlfluffignore"),
        os.path.join(project_dir, "dbt", ".sqlfluffignore"),
    )

    rmtree(os.path.join(project_dir, "dbt", "macros", "dbt"))

    shutil.copytree(
        os.path.join(TEMPLATE_PROJECT_DIR, "dbt", "macros", "dbt"),
        os.path.join(project_dir, "dbt", "macros", "dbt"),
    )
