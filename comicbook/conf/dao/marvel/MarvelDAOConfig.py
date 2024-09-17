from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType, ArrayType


@dataclass(frozen=True)
class MarvelDAOConfig:
    path: str = None
    schema: StructType = StructType([
        StructField("page_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("urlslug", StringType(), True),
        StructField("id", StringType(), True),
        StructField("eye", StringType(), True),
        StructField("hair", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("gsm", StringType(), True),
        StructField("alive", StringType(), True),
        StructField("appearances", IntegerType(), True),
        StructField("first appearance", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

