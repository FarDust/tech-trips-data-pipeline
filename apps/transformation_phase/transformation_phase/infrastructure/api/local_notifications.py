import logging

from landing_phase.domain.ports.notifications_port import NotificationsPort

logging.basicConfig(level=logging.INFO)


class LocalNotifications(NotificationsPort):

    __LOGGER = logging.getLogger(__name__)

    def notify(self, message: str) -> None:
        self.__LOGGER.info(message)
