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
TRANSFORMED_PATH = environ.get("TRANSFORMED_PATH", "transformed")
PROCESSED_PATH = environ.get("PROCESSED_PATH", "processed")
PROCESSING_PATH = environ.get("PROCESSING_PATH", "landing")

MODEL_RANDOM_STATE = 42
MODEL_MAX_CLUSTERS = 20
MODEL_MAX_ITERS = 1000
