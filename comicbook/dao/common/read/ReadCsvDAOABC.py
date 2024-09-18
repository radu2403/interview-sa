from abc import ABC
from pyspark.sql import DataFrame
from dataclasses import dataclass, field

from comicbook.conf.dao.common.ReadDAOConfig import ReadDAOConfig
from comicbook.dao.common.DAOABCBase import DaoBaseABC


@dataclass(frozen=True)
class ReadCsvDAOABC(DaoBaseABC, ABC):
    _config: ReadDAOConfig = field(repr=False, default=None)

    def load(self) -> DataFrame:
        return (self._spark_service.spark.read
                .format("csv")
                .option("mode", "DROPMALFORMED")
                .option("mergeSchema", "true")
                .schema(self._config.schema)
                .option("header", True)
                .load(self._config.path)
                )