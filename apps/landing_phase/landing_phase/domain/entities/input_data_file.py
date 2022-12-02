from dataclasses import dataclass

from pandas.io.parsers import TextFileReader


@dataclass
class InputDataFile:
    name: str
    bucket: str
    data: TextFileReader
