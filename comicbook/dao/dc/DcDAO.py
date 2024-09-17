from dataclasses import dataclass, field
from pyspark.sql import DataFrame

from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.dao.common.DAOABCBase import DaoABC


@dataclass(frozen=True)
class DcDAO(DaoABC):
    _config: DcDAOConfig = field(repr=False, default=None)

    def load(self) -> DataFrame:
        return (self._spark_service.spark.read
                                    .format("csv")
                                    .option("mode", "DROPMALFORMED")
                                    .option("mergeSchema", "true")
                                    .schema(self._config.schema)
                                    .option("header", True)
                                    .load(self._config.path)
         )

    def write(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("write is not implemented for StatsDAO")
