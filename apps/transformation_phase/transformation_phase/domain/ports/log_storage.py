from abc import ABCMeta, abstractmethod

from transformation_phase.domain.entities.log_data import LogData


class LogStorage(metaclass=ABCMeta):
    @abstractmethod
    def create(self, entity: LogData) -> LogData:
        pass

    @abstractmethod
    def delete(self, entity: LogData) -> bool:
        pass
