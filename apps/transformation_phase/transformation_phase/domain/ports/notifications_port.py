from abc import ABCMeta, abstractmethod


class NotificationsPort(metaclass=ABCMeta):
    @abstractmethod
    def notify(self, message: str) -> None:
        pass
