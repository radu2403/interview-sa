from abc import ABC
from typing import Callable

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, trim


@dataclass(frozen=True)
class CleanHeroNamesJobTransformation(ABC):

    @staticmethod
    def clean_column_name_transformation(col_name: str) -> Callable[[DataFrame], DataFrame]:

        def _(df: DataFrame) -> DataFrame:
            return (df
                      # Remove words in parentheses
                      .withColumn(col_name, regexp_replace(col(col_name), r"\(.*?\)", ""))

                      # Replace multiple spaces with a single space
                      .withColumn(col_name, regexp_replace(col(col_name), r"\s+", " "))

                      # Trim leading and trailing spaces (if any)
                      .withColumn(col_name, trim(col(col_name)))
                    )

        return _