from dataclasses import field, dataclass
from pyspark.sql.dataframe import DataFrame
from abc import ABC, abstractmethod
from comicbook.service.SparkService import SparkServiceABC, SparkService


@dataclass(frozen=True)
class DaoBaseABC(ABC):
    # private
    _spark_service: SparkServiceABC = field(repr=False, default=SparkService())

    def __post_init__(self):
        pass


@dataclass(frozen=True)
class DaoABC(DaoBaseABC, ABC):

    def __post_init__(self):
        super().__post_init__()
        
    @abstractmethod
    def load(self) -> DataFrame:
        """Main load method"""

    @abstractmethod
    def write(self, df: DataFrame) -> DataFrame:
        """Main write method"""

    def drop(self):
        """Main drop method"""
        raise NotImplementedError("This method was not implemented")
