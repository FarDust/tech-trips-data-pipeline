from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class AbstractStorage(Generic[T], metaclass=ABCMeta):
    @abstractmethod
    def create(self, entity: T) -> T:
        pass

    @abstractmethod
    def find(self, entity: T) -> T:
        pass
