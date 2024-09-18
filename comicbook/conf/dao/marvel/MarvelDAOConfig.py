from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType, ArrayType

from comicbook.common.conventions.dataset.marvel.MarvelConst import MarvelConst as MC
from comicbook.conf.dao.common.ReadDAOConfig import ReadDAOConfig


@dataclass(frozen=True)
class MarvelDAOConfig(ReadDAOConfig):
    path: str = None
    schema: StructType = StructType([
        StructField(MC.page_id, IntegerType(), True),
        StructField(MC.name, StringType(), True),
        StructField(MC.urls_lug, StringType(), True),
        StructField(MC.id, StringType(), True),
        StructField(MC.eye, StringType(), True),
        StructField(MC.hair, StringType(), True),
        StructField(MC.sex, StringType(), True),
        StructField(MC.gsm, StringType(), True),
        StructField(MC.alive, StringType(), True),
        StructField(MC.appearances, IntegerType(), True),
        StructField(MC.first_appearance, StringType(), True),
        StructField(MC.year, IntegerType(), True),
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

