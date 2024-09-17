from dataclasses import dataclass
from abc import ABC

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class SparkServiceABC(ABC):
    spark: SparkSession


@dataclass(frozen=True)
class SparkService(SparkServiceABC):
    spark: SparkSession = None

    def __post_init__(self):
        if not self.spark: object.__setattr__(self, "spark", SparkSession.builder.appName("olympus").getOrCreate())

        self.spark.sql("set spark.sql.datetime.java8API.enabled = true")
        self.spark.sql("SET TIME ZONE '+00:00'")
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
