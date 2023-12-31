from functools import wraps

import asyncio


# https://github.com/tiangolo/typer/issues/85#issuecomment-1365871959
# https://github.com/tiangolo/typer/issues/88
def typer_async(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper
