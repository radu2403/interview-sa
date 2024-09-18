from dataclasses import dataclass
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, BooleanType, TimestampType, IntegerType, ArrayType

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.conf.dao.common.ReadDAOConfig import ReadDAOConfig


@dataclass(frozen=True)
class HeroAbilityDAOConfig(ReadDAOConfig):
    path: str = None
    schema: StructType = StructType([
        StructField(HC.name, StringType(), True),
        StructField(HC.real_name, StringType(), True),
        StructField(HC.full_name, StringType(), True),
        StructField(HC.overall_score, StringType(), True),
        StructField(HC.history_text, StringType(), True),
        StructField(HC.powers_text, StringType(), True),
        StructField(HC.intelligence_score, IntegerType(), True),
        StructField(HC.strength_score, IntegerType(), True),
        StructField(HC.speed_score, IntegerType(), True),
        StructField(HC.durability_score, IntegerType(), True),
        StructField(HC.power_score, IntegerType(), True),
        StructField(HC.combat_score, IntegerType(), True),
        StructField(HC.superpowers, StringType(), True),
        StructField(HC.alter_egos, StringType(), True),
        StructField(HC.aliases, StringType(), True),
        StructField(HC.place_of_birth, StringType(), True),
        StructField(HC.first_appearance, StringType(), True),
        StructField(HC.occupation, StringType(), True),
        StructField(HC.base, StringType(), True),
        StructField(HC.teams, StringType(), True),
        StructField(HC.relatives, StringType(), True),
        StructField(HC.gender, StringType(), True),
        StructField(HC.type_race, StringType(), True),
        StructField(HC.height, StringType(), True),
        StructField(HC.weight, StringType(), True),
        StructField(HC.eye_color, StringType(), True),
        StructField(HC.hair_color, StringType(), True),
        StructField(HC.skin_color, StringType(), True),
        StructField(HC.id, LongType(), True),
        StructField(HC.prompt, StringType(), True),

    ])

    def __post_init__(self):
        if not self.path:
            object.__setattr__(self, "path", "default_path_from_config")

