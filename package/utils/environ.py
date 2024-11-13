import os


def get_env_var(key, default=None):
    value = os.environ.get(key)
    if value is None:
        if default is None:
            raise ValueError(f"Environment variable '{key}' not found.")
        else:
            return default
    else:
        return value
