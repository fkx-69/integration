"""Database connection helpers for the PostgreSQL sales analysis layer."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional convenience dependency
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(DOTENV_PATH, override=False)


def build_database_url() -> str:
    """Build the SQLAlchemy PostgreSQL URL from environment variables."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    settings = {
        "PGHOST": os.getenv("PGHOST", "localhost"),
        "PGPORT": os.getenv("PGPORT", "5432"),
        "PGDATABASE": os.getenv("PGDATABASE"),
        "PGUSER": os.getenv("PGUSER"),
        "PGPASSWORD": os.getenv("PGPASSWORD"),
    }
    missing = [name for name in ("PGDATABASE", "PGUSER", "PGPASSWORD") if not settings[name]]
    if missing:
        raise ValueError(
            "Missing PostgreSQL settings. Set DATABASE_URL or the following variables: "
            + ", ".join(sorted(missing))
        )

    try:
        port = int(settings["PGPORT"])
    except ValueError as exc:
        raise ValueError("PGPORT must be an integer.") from exc

    url = URL.create(
        drivername="postgresql+psycopg",
        username=settings["PGUSER"],
        password=settings["PGPASSWORD"],
        host=settings["PGHOST"],
        port=port,
        database=settings["PGDATABASE"],
    )
    return url.render_as_string(hide_password=False)


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return a cached SQLAlchemy engine for PostgreSQL."""
    return create_engine(build_database_url(), pool_pre_ping=True, future=True)


def test_connection() -> None:
    """Raise an exception if the PostgreSQL connection is not available."""
    with get_engine().connect() as connection:
        connection.execute(text("SELECT 1"))
