import functions_framework
from google.cloud.functions.context import Context
from mean_trips.constants.environment import DB_CONNECTION_URL
from mean_trips.infrastructure.repositories.trips_weekly_metric_sql_repository import (
    TripWeeklyMetricSQLRepository,
)
from mean_trips.use_cases.trips_weekly_metric_use_case import TripsWeeklyMetricUseCase
from sqlalchemy import create_engine


@functions_framework.http
def trips_metrics(http_event, context: "Context"):

    payload = http_event.get_json()

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
    return result
