from dataclasses import dataclass
from typing import Generator, Optional

from pandas import DataFrame


@dataclass
class InputDataFile:
    name: str
    bucket: str
    data: Optional[Generator[DataFrame, None, None]] = None
