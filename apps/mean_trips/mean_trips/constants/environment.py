from os import environ
from pathlib import Path

DB_CONNECTION_URL = environ.get(
    "DB_CONNECTION_URL",
    (Path(__file__) / "../../../../data/processed.db")
    .resolve()
    .absolute()
    .as_uri()
    .replace("file://", "sqlite://"),
)
