from abc import ABC, abstractmethod

class SQLCommand(ABC):
    @property
    @abstractmethod
    def label(self) -> str: ...

    @property
    def commit(self) -> bool:
        return True
    
    @property
    def params(self) -> dict:
        {}
    
    @property
    @abstractmethod
    def query(self) -> str: ...
