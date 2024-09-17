from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType, ArrayType


@dataclass(frozen=True)
class HeroAbilityDAOConfig:
    path: str = None
    schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("real_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("overall_score", StringType(), True),
        StructField("history_text", StringType(), True),
        StructField("powers_text", StringType(), True),
        StructField("intelligence_score", IntegerType(), True),
        StructField("strength_score", IntegerType(), True),
        StructField("speed_score", IntegerType(), True),
        StructField("durability_score", IntegerType(), True),
        StructField("power_score", IntegerType(), True),
        StructField("combat_score", IntegerType(), True),
        StructField("superpowers", StringType(), True),
        StructField("alter_egos", StringType(), True),
        StructField("aliases", StringType(), True),
        StructField("place_of_birth", StringType(), True),
        StructField("first_appearance", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("base", StringType(), True),
        StructField("teams", StringType(), True),
        StructField("relatives", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("type_race", StringType(), True),
        StructField("height", StringType(), True),
        StructField("weight", StringType(), True),
        StructField("eye_color", StringType(), True),
        StructField("hair_color", StringType(), True),
        StructField("skin_color", StringType(), True),
        StructField("id", LongType(), True),
        StructField("prompt", StringType(), True),

    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

