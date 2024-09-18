from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import from_json

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.dao.common.DAOABCBase import DaoABC
from comicbook.dao.common.read.ReadCsvDAOABC import ReadCsvDAOABC


@dataclass(frozen=True)
class HeroAbilityDAO(ReadCsvDAOABC, DaoABC):
    _config: HeroAbilityDAOConfig = field(repr=False, default=None)

    def load(self) -> DataFrame:
        array_schema = ArrayType(StringType())

        return (
            super().load()
                   .withColumn(HC.superpowers, from_json(HC.superpowers, schema=array_schema))
        )

    def write(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("write is not implemented for StatsDAO")
