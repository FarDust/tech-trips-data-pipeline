from abc import ABCMeta, abstractmethod

from mean_trips.domain.models.trip_weely_metric import TripWeeklyMetric


class TripWeeklyMetricRepository(metaclass=ABCMeta):
    @abstractmethod
    def find(self, entity: TripWeeklyMetric) -> TripWeeklyMetric:
        pass
