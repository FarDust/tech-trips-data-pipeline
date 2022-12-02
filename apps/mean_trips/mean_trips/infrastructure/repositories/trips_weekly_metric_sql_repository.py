import logging

from mean_trips.constants.columns import Columns
from mean_trips.domain.models.trip_weely_metric import TripWeeklyMetric
from mean_trips.domain.ports.trip_weekly_metric_repository import (
    TripWeeklyMetricRepository,
)
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Identity,
    Integer,
    MetaData,
    Table,
    Text,
    inspect,
)
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO)


class TripWeeklyMetricSQLRepository(TripWeeklyMetricRepository):

    __LOGGER = logging.getLogger(__name__)

    def __init__(self, engine: Engine, table="trips") -> None:
        self.engine = engine
        self.table_name = table
        if not inspect(self.engine).has_table(self.table_name):
            metadata = MetaData(engine)
            self.table = Table(
                self.table_name,
                metadata,
                Column("pk", Integer, Identity(), primary_key=True),
                Column(Columns.REGION, Text),
                Column(Columns.HOUR, Integer),
                Column(Columns.ORIGIN_LAT, Float),
                Column(Columns.ORIGIN_LON, Float),
                Column(Columns.DESTINATION_LAT, Float),
                Column(Columns.DESTINATION_LON, Float),
                Column(Columns.DATETIME, DateTime),
            )
            metadata.create_all()
        else:
            self.table = Table(self.table_name, MetaData(self.engine))

    def find(self, entity: TripWeeklyMetric) -> TripWeeklyMetric:
        query = f"""
        with weekly_trips as (
            SELECT strftime('%W', datetime) WeekNumber, count(*) as trips_per_week
            from trips as t
            where region == '{entity['region']}'
            and origin_lat between {entity['min_lat']} and {entity['max_lat']}
            and origin_lon between {entity['min_lon']} and {entity['max_lon']}
            and destination_lat between {entity['min_lat']} and {entity['max_lat']}
            and destination_lon between {entity['min_lon']} and {entity['max_lon']}
            GROUP by WeekNumber
        )
        SELECT AVG(trips_per_week) from weekly_trips;
        """

        self.__LOGGER.info(f"Executing query: {query}")

        result = self.engine.execute(query).fetchone()

        return TripWeeklyMetric(
            min_lat=entity["min_lat"],
            max_lat=entity["max_lat"],
            min_lon=entity["min_lon"],
            max_lon=entity["max_lon"],
            trips=result[0],
            region=entity["region"],
        )
