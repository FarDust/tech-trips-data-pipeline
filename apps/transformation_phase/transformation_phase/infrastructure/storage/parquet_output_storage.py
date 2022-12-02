from hashlib import md5
from pathlib import Path

from pandas import read_parquet
from pandas.util import hash_pandas_object
from pyarrow.lib import ArrowInvalid
from transformation_phase.constants.columns import PARTITION_COLS
from transformation_phase.constants.environment import TRANSFORMED_PATH
from transformation_phase.constants.storage import (
    DEFAULT_PROTOCOL,
    URI_EQUAL_SIGN,
    ProtocolType,
)
from transformation_phase.domain.entities.output_data_file import OutputDataFile
from transformation_phase.domain.ports.output_storage_port import OutputStorage
from transformation_phase.infrastructure.storage.shared.base_storage import (
    AbstractStorage,
)


class ParquetTripsOutputStorage(AbstractStorage[OutputDataFile], OutputStorage):
    """Storage class for output files."""

    def __init__(self, bucket_name: str, protocol: ProtocolType) -> None:
        self.bucket_name = bucket_name
        self.protocol = protocol

    def create(self, entity: OutputDataFile) -> OutputDataFile:
        object_hash = md5(hash_pandas_object(entity.data).values).hexdigest()
        new_path = (
            Path(TRANSFORMED_PATH)
            / str(Path(entity.name).with_suffix(""))
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
        except (FileNotFoundError, ArrowInvalid):
            entity.data.to_parquet(
                object_directory,
                index=False,
                partition_cols=list(
                    filter(
                        lambda partition: partition in entity.data.columns,
                        PARTITION_COLS,
                    )
                ),
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
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
