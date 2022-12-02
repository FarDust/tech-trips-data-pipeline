import logging

from mean_trips.constants.environment import DB_CONNECTION_URL
from mean_trips.domain.models.trip_weely_metric import TripWeeklyMetric
from mean_trips.infrastructure.repositories.trips_weekly_metric_sql_repository import (
    TripWeeklyMetricSQLRepository,
)
from mean_trips.use_cases.trips_weekly_metric_use_case import TripsWeeklyMetricUseCase
from sqlalchemy import create_engine

if __name__ == "__main__":

    payload: TripWeeklyMetric = {
        "region": "Hamburg",
        "min_lat": 53.4,
        "max_lat": 53.7,
        "min_lon": 9.5,
        "max_lon": 10.2,
    }

    use_cases = {
        "trips_weekly": TripsWeeklyMetricUseCase(
            repository=TripWeeklyMetricSQLRepository(
                engine=create_engine(DB_CONNECTION_URL),
            ),
        )
    }

    metric_use_case = use_cases["trips_weekly"]
    result = metric_use_case.execute(
        entity=payload,
    )
    logging.info(result)
