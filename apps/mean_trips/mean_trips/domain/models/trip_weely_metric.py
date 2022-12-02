from typing import TypedDict


class TripWeeklyMetric(TypedDict, total=False):
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    trips: int
    region: str
