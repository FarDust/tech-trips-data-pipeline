from abc import ABCMeta, abstractmethod

from transformation_phase.domain.entities.classification_data import ClassificationData


class ClusteringAPI(metaclass=ABCMeta):
    @abstractmethod
    def classify(self, entity: ClassificationData) -> ClassificationData:
        pass

    @abstractmethod
    def train(self, entity: ClassificationData) -> None:
        pass
