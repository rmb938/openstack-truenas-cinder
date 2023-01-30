from dataclasses import dataclass
from enum import Enum, auto


class DatasetType(Enum):
    FILESYSTEM = auto()
    VOLUME = auto()


@dataclass
class Dataset:
    id: str
    type: DatasetType
    size: int
    used: int
    origin: str
