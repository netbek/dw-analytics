from functools import lru_cache
from package.config.constants import PYTHON_KEYWORDS


@lru_cache
def is_python_keyword(value: str) -> bool:
    return value.lower() in PYTHON_KEYWORDS
