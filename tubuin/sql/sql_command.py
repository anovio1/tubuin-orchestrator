from abc import ABC, abstractmethod
from typing import LiteralString

class SQLCommand(ABC):
    label: str
    query: str
    commit: bool = True
    params: dict = {}
