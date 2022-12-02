from abc import ABCMeta, abstractmethod

from transformation_phase.domain.entities.output_data_file import OutputDataFile


class OutputStorage(OutputDataFile, metaclass=ABCMeta):
    @abstractmethod
    def create(self, entity: OutputDataFile) -> OutputDataFile:
        pass

    @abstractmethod
    def find(self, entity: OutputDataFile) -> OutputDataFile:
        pass
