from hashlib import md5
from pathlib import Path

from landing_phase.constants.columns import PARTITION_COLS
from landing_phase.constants.environment import PROCESSED_PATH
from landing_phase.constants.storage import (
    DEFAULT_PROTOCOL,
    URI_EQUAL_SIGN,
    ProtocolType,
)
from landing_phase.domain.entities.output_data_file import OutputDataFile
from landing_phase.domain.ports.output_storage_port import OutputStorage
from landing_phase.infrastructure.storage.shared.base_storage import AbstractStorage
from pandas import read_parquet
from pandas.util import hash_pandas_object


class OutputFileStorage(AbstractStorage[OutputDataFile], OutputStorage):
    """Storage class for output files."""

    def __init__(self, bucket_name: str, protocol: ProtocolType) -> None:
        self.bucket_name = bucket_name
        self.protocol = protocol

    def create(self, entity: OutputDataFile) -> OutputDataFile:
        object_hash = md5(hash_pandas_object(entity.data).values).hexdigest()
        new_path = (
            Path(PROCESSED_PATH)
            / f"source={str(Path(entity.name).with_suffix(''))}"
            / f"hash={object_hash}"
        )
        object_directory = (
            (Path(self.bucket_name) / new_path)
            .as_uri()
            .replace(DEFAULT_PROTOCOL, self.protocol)
            .replace(URI_EQUAL_SIGN, "=")
        )
        try:
            read_parquet(object_directory)
            raise FileExistsError(
                f"File {object_directory} already exists. Skipping creation."
            )
        except FileNotFoundError:
            entity.data.to_parquet(
                object_directory,
                index=False,
                partition_cols=list(
                    filter(
                        lambda partition: partition in entity.data.columns,
                        PARTITION_COLS,
                    )
                ),
            )
        return OutputDataFile(name=str(new_path), data=entity.data)

    def find(self, entity: OutputDataFile) -> OutputDataFile:
        return OutputDataFile(
            name=entity.name,
            data=read_parquet(
                str(
                    Path(f"{self.bucket_name}/{entity.name}")
                    .as_uri()
                    .replace(DEFAULT_PROTOCOL, self.protocol)
                )
            ),
        )
