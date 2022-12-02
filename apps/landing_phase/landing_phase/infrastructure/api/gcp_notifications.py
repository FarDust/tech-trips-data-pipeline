import logging

from google.cloud.pubsub_v1 import PublisherClient
from landing_phase.domain.ports.notifications_port import NotificationsPort

logging.basicConfig(level=logging.INFO)


class PubSubNotifications(NotificationsPort):

    __LOGGER = logging.getLogger(__name__)

    def __init__(self, project_id: str, topic_id: str) -> None:
        self.__project_id = project_id
        self.__topic_id = topic_id
        self.publisher = PublisherClient()
        self.topic_path = self.publisher.topic_path(self.__project_id, self.__topic_id)

    def notify(self, message: str) -> None:

        self.publisher.publish(self.topic_path, data=message.encode("utf-8"))

        self.__LOGGER.info(message)
