from json import dumps
from typing import Generator

from transformation_phase.domain.entities.classification_data import ClassificationData
from transformation_phase.domain.entities.input_data_file import InputDataFile
from transformation_phase.domain.entities.output_data_file import OutputDataFile
from transformation_phase.domain.ports.clustering_api_port import ClusteringAPI
from transformation_phase.domain.ports.input_storage_port import InputStorage
from transformation_phase.domain.ports.notifications_port import NotificationsPort


class ClassifyUseCase:
    def __init__(
        self,
        input_storage: InputStorage,
        clustering_api: ClusteringAPI,
        notifications_api: NotificationsPort,
    ):
        self.input_storage = input_storage
        self.clustering_api = clustering_api
        self.notifications_api = notifications_api

    def classify(self, bucket: str, name: str) -> Generator[OutputDataFile, None, None]:

        input_file = InputDataFile(name=name, bucket=bucket, data=None)

        input_file_with_data = self.input_storage.find(input_file)

        self.notifications_api.notify(
            dumps(
                {
                    "code": "CLASSIFYING",
                    "message": "Received file",
                    "file": input_file_with_data.name,
                }
            )
        )

        if input_file_with_data.data is None:
            raise ValueError("Input file has no data")

        for chunk in input_file_with_data.data:
            self.clustering_api.train(ClassificationData(data=chunk))
            self.notifications_api.notify(
                dumps(
                    {
                        "code": "TRAINED",
                        "message": f"Chunk of size {chunk.memory_usage().sum() * 1e-6} MB classified",
                        "file": input_file_with_data.name,
                    }
                )
            )

        classification_file = self.input_storage.find(input_file)

        if classification_file.data is None:
            raise ValueError("Classification file has no data")

        for chunk in classification_file.data:
            output_classification = self.clustering_api.classify(
                ClassificationData(data=chunk)
            )

            self.notifications_api.notify(
                dumps(
                    {
                        "code": "CLASSIFIED",
                        "message": f"Chunk of size {chunk.memory_usage().sum() * 1e-6} MB classified",
                        "file": classification_file.name,
                    }
                )
            )

            yield OutputDataFile(
                name=input_file_with_data.name,
                data=output_classification.data,
            )
