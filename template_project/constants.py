from package.constants import DBT_PROFILES_DIR  # noqa: F401

import os

DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbt")

NOTEBOOKS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebooks")
