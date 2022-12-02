from enum import Enum


class Columns(str, Enum):
    """Enum class for columns."""

    DATASOURCE = "datasource"
    REGION = "region"
    CLUSTER = "cluster"
    DATETIME = "datetime"
    ORIGIN_COORD = "origin_coord"
    DESTINATION_COORD = "destination_coord"
    ORIGIN_LAT = "origin_lat"
    ORIGIN_LON = "origin_lon"
    DESTINATION_LAT = "destination_lat"
    DESTINATION_LON = "destination_lon"
    HOUR = "hour"
    DISTANCE = "distance"

    def __str__(self) -> str:
        return str(self.value)


PARTITION_COLS = [Columns.REGION, Columns.HOUR]
INGESTION_COLS = [
    Columns.DATASOURCE,
    Columns.REGION,
    Columns.DATETIME,
    Columns.ORIGIN_COORD,
    Columns.DESTINATION_COORD,
]
