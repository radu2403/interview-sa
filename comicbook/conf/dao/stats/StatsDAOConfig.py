from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType


@dataclass(frozen=True)
class StatsDAOConfig:
    path: str = None
    schema: StructType = StructType([
        StructField("ID", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Alignment", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("EyeColor", StringType(), True),
        StructField("Race", StringType(), True),
        StructField("HairColor", StringType(), True),
        StructField("Publisher", StringType(), True),
        StructField("SkinColor", StringType(), True),
        StructField("Height", DoubleType(), True),
        StructField("Weight", DoubleType(), True)
    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

