from enum import Enum


class Columns(str, Enum):
    """Enum class for columns."""

    REGION = "region"
    DATETIME = "datetime"
    ORIGIN_LAT = "origin_lat"
    ORIGIN_LON = "origin_lon"
    DESTINATION_LAT = "destination_lat"
    DESTINATION_LON = "destination_lon"
    HOUR = "hour"

    def __str__(self) -> str:
        return str(self.value)
