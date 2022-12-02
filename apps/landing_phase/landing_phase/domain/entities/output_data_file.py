from dataclasses import dataclass

from pandas import DataFrame


@dataclass
class OutputDataFile:
    name: str
    data: DataFrame
