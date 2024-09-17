

from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType

from comicbook.common.conventions.dataset.dc.DcConst import DcConst as DC


@dataclass(frozen=True)
class DcDAOConfig:
    path: str = None
    schema: StructType = StructType([
        StructField(DC.page_id, LongType(), True),
        StructField(DC.name, StringType(), True),
        StructField(DC.urls_lug, StringType(), True),
        StructField(DC.id, StringType(), True),
        StructField(DC.eye, StringType(), True),
        StructField(DC.hair, StringType(), True),
        StructField(DC.sex, StringType(), True),
        StructField(DC.gsm, StringType(), True),
        StructField(DC.alive, StringType(), True),
        StructField(DC.appearances, IntegerType(), True),
        StructField(DC.first_appearance, StringType(), True),
        StructField(DC.year, IntegerType(), True),
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

