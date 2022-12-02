from pathlib import Path

from landing_phase.constants.environment import PROCESSING_PATH
from landing_phase.constants.storage import (
    CSV_CHUNK_SIZE,
    DEFAULT_PROTOCOL,
    ProtocolType,
)
from landing_phase.domain.entities.input_data_file import InputDataFile
from landing_phase.domain.ports.input_storage_port import InputStorage
from landing_phase.infrastructure.storage.shared.base_storage import AbstractStorage
from pandas import read_csv


class InputFileStorage(AbstractStorage[InputDataFile], InputStorage):
    """Storage class for output files."""

    def __init__(self, protocol: ProtocolType) -> None:
        self.protocol = protocol

    def create(self, _entity: InputDataFile) -> InputDataFile:
        raise NotImplementedError()

    def find(self, entity: InputDataFile) -> InputDataFile:
        if f"/{PROCESSING_PATH}/" in entity.name:
            new_name = entity.name
        else:
            new_name = f"{PROCESSING_PATH}/{entity.name}"
        return InputDataFile(
            name=new_name,
            bucket=entity.bucket,
            data=read_csv(
                (Path(entity.bucket) / new_name)
                .as_uri()
                .replace(DEFAULT_PROTOCOL, self.protocol),
                chunksize=CSV_CHUNK_SIZE,
            ),
        )
