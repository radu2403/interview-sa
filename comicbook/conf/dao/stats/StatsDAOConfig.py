from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType

from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.common.ReadDAOConfig import ReadDAOConfig


@dataclass(frozen=True)
class StatsDAOConfig(ReadDAOConfig):
    path: str = None
    schema: StructType = StructType([
        StructField(SC.id, LongType(), True),
        StructField(SC.name, StringType(), True),
        StructField(SC.alignment, StringType(), True),
        StructField(SC.gender, StringType(), True),
        StructField(SC.eye_color, StringType(), True),
        StructField(SC.race, StringType(), True),
        StructField(SC.hair_color, StringType(), True),
        StructField(SC.publisher, StringType(), True),
        StructField(SC.skin_color, StringType(), True),
        StructField(SC.height, DoubleType(), True),
        StructField(SC.weight, DoubleType(), True)
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

