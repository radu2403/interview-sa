from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.common.DAOABCBase import DaoABC
from comicbook.dao.common.read.ReadCsvDAOABC import ReadCsvDAOABC


@dataclass(frozen=True)
class StatsDAO(ReadCsvDAOABC, DaoABC):
    _config: StatsDAOConfig = field(repr=False, default=None)

    def write(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("write is not implemented for StatsDAO")
