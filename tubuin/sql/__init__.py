# tubuin/sql/__init__.py
from pathlib import Path
from importlib.resources import files


SQL_DIR = Path(__file__).parent


def load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    with path.open("r", encoding="utf-8") as f:
        return f.read()

# collect all of the .sql filenames in this directory
_SQL_FILES = {
    p.stem.replace(".", "_").lower(): p.name for p in Path(__file__).parent.glob("*.sql")
}

__all__ = list(_SQL_FILES)  # for “from sql import *”


def __getattr__(name: str) -> str:
    """
    When someone does `import sql; sql.some_query`, Python will call this
    if `some_query` isn't already defined.  We map it to the appropriate .sql file.
    """
    try:
        filename = _SQL_FILES[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return (files(__package__) / filename).read_text(encoding="utf-8")


def __dir__():
    """
    Include our dynamic attributes in autocomplete.
    """
    return sorted(list(globals().keys()) + list(_SQL_FILES.keys()))
