from typing import Dict, Union

from mean_trips.domain.models.trip_weely_metric import TripWeeklyMetric
from mean_trips.domain.ports.trip_weekly_metric_repository import (
    TripWeeklyMetricRepository,
)


class TripsWeeklyMetricUseCase:
    def __init__(
        self,
        repository: TripWeeklyMetricRepository,
    ) -> None:
        self.repository = repository

    def execute(self, entity: TripWeeklyMetric) -> Dict[str, Union[float, str]]:
        result = self.repository.find(entity)
        return {
            "mean_trips": result["trips"],
            "region": result["region"],
            "min_lat": result["min_lat"],
            "max_lat": result["max_lat"],
            "min_lon": result["min_lon"],
            "max_lon": result["max_lon"],
        }
