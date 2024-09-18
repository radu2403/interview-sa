from dataclasses import dataclass, field
from pyspark.sql import DataFrame

from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.dao.common.DAOABCBase import DaoABC
from comicbook.dao.common.read.ReadCsvDAOABC import ReadCsvDAOABC


@dataclass(frozen=True)
class DcDAO(ReadCsvDAOABC, DaoABC):
    _config: DcDAOConfig = field(repr=False, default=None)

    def write(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("write is not implemented for StatsDAO")
