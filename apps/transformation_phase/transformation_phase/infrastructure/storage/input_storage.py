from pathlib import Path
from typing import Generator

from pandas import DataFrame, read_parquet
from pyarrow.parquet import ParquetDataset
from transformation_phase.constants.columns import INGESTION_COLS, Columns
from transformation_phase.constants.environment import PROCESSED_PATH
from transformation_phase.constants.storage import (
    DEFAULT_PROTOCOL,
    URI_EQUAL_SIGN,
    ProtocolType,
)
from transformation_phase.domain.entities.input_data_file import InputDataFile
from transformation_phase.domain.ports.input_storage_port import InputStorage
from transformation_phase.infrastructure.storage.shared.base_storage import (
    AbstractStorage,
)
from transformation_phase.utils.path import get_region_from_path


class InputFileStorage(AbstractStorage[InputDataFile], InputStorage):
    """Storage class for input files."""

    def __init__(self, protocol: ProtocolType) -> None:
        self.protocol = protocol

    def create(self, _entity: InputDataFile) -> InputDataFile:
        raise NotImplementedError()

    def find(self, entity: InputDataFile) -> InputDataFile:
        if f"/{PROCESSED_PATH}/" in entity.name:
            new_name = entity.name
        else:
            new_name = f"{PROCESSED_PATH}/{entity.name}"

        file_uri = (
            (Path(entity.bucket) / new_name)
            .as_uri()
            .replace(URI_EQUAL_SIGN, "=")
            .replace(DEFAULT_PROTOCOL, self.protocol)
        )

        new_trips_dataset = ParquetDataset(
            file_uri,
            use_legacy_dataset=False,
        )

        def iter_chunks() -> Generator[DataFrame, None, None]:
            for fragment in new_trips_dataset.fragments:
                region = get_region_from_path(fragment.path)
                skipped_columns = set()
                if region != "":
                    skipped_columns.add(Columns.REGION)
                chunk_df = fragment.to_table(
                    columns=list(
                        filter(lambda x: not (x in skipped_columns), INGESTION_COLS)
                    )
                ).to_pandas()
                if region != "":
                    chunk_df[Columns.REGION] = region
                yield chunk_df

        chunk_generator = iter_chunks()

        return InputDataFile(
            name=new_name,
            bucket=entity.bucket,
            data=chunk_generator,
        )
