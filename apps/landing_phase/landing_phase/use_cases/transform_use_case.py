from json import dumps
from pathlib import Path

from landing_phase.constants.environment import PROCESSING_PATH
from landing_phase.domain.entities.input_data_file import InputDataFile
from landing_phase.domain.entities.output_data_file import OutputDataFile
from landing_phase.domain.ports.input_storage_port import InputStorage
from landing_phase.domain.ports.notifications_port import NotificationsPort
from landing_phase.domain.ports.output_storage_port import OutputStorage


class TransformUseCase:
    def __init__(
        self,
        input_storage: InputStorage,
        output_storage: OutputStorage,
        notifications_api: NotificationsPort,
    ) -> None:
        self.input_storage = input_storage
        self.output_storage = output_storage
        self.notifications_api = notifications_api

    def transform(self, bucket: str, name: str) -> None:
        input_file = InputDataFile(name=name, bucket=bucket, data=None)

        input_file_with_data = self.input_storage.find(input_file)

        self.notifications_api.notify(
            dumps(
                {
                    "code": "TRANSFORMING",
                    "message": "Received file",
                    "file": input_file_with_data.name,
                }
            )
        )

        base_path = Path(input_file_with_data.name.replace(f"{PROCESSING_PATH}/", ""))
        new_path_name = base_path.with_stem(f"{Path(base_path.name).stem}_transformed")

        for chunk in input_file_with_data.data:
            try:
                new_data = self.output_storage.create(
                    OutputDataFile(name=str(new_path_name), data=chunk)
                )

                self.notifications_api.notify(
                    dumps(
                        {
                            "code": "TRANSFORMED",
                            "message": f"Chunk of size {new_data.data.memory_usage().sum() * 1e-6} MB transformed",
                            "file": str(new_data.name),
                        }
                    ),
                )
            except FileExistsError:
                self.notifications_api.notify(
                    dumps(
                        {
                            "code": "TRANSFORMED",
                            "message": f"File {new_path_name.absolute().as_uri()} already exists",
                            "file": str(new_path_name),
                        }
                    ),
                )
