

from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType


@dataclass(frozen=True)
class DcDAOConfig:
    path: str = None
    schema: StructType = StructType([
        StructField("page_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("urlslug", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("EYE", StringType(), True),
        StructField("HAIR", StringType(), True),
        StructField("SEX", StringType(), True),
        StructField("GSM", StringType(), True),
        StructField("ALIVE", StringType(), True),
        StructField("APPEARANCES", IntegerType(), True),
        StructField("FIRST APPEARANCE", StringType(), True),
        StructField("YEAR", IntegerType(), True),
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

