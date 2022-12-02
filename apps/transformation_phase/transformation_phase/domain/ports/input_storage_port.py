from abc import ABCMeta, abstractmethod

from transformation_phase.domain.entities.input_data_file import InputDataFile


class InputStorage(metaclass=ABCMeta):
    @abstractmethod
    def find(self, entity: InputDataFile) -> InputDataFile:
        pass
