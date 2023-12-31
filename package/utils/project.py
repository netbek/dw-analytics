from package.constants import PROJECTS_DIR

import os


def list_projects() -> [str]:
    return list(os.listdir(PROJECTS_DIR))


def project_exists(project_name: str) -> bool:
    return project_name in list_projects()


def get_project_dir(project_name: str) -> str:
    return os.path.join(PROJECTS_DIR, project_name)
