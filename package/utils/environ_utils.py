import os


def get_env_var(key):
    value = os.environ.get(key)
    if value is None:
        raise ValueError(f"Environment variable '{key}' not found.")
    return value
