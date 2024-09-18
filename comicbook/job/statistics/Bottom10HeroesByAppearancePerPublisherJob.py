from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from comicbook.common.conventions.dataset.stats.AlignmentTypesConst import AlignmentTypesConst
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.common.conventions.dataset.marvel.MarvelConst import MarvelConst as MC
from comicbook.job.statistics.StatisticsJobBase import StatisticsJobBase


@dataclass(frozen=True)
class Bottom10HeroesByAppearancePerPublisherJob(StatisticsJobBase):

    def transformation(self, df_marvel: DataFrame, df_dc: DataFrame) -> Callable[[DataFrame], DataFrame]:
        super_tr = super().transformation(df_marvel, df_dc)

        def _(df_stats: DataFrame) -> DataFrame:
            w = Window.partitionBy(SC.publisher).orderBy(col(MC.appearances).asc())

            return (
                df_stats.transform(super_tr)
                        .withColumn("row", row_number().over(w))
                        .where(col("row") <= 10)
                        .orderBy(col(MC.appearances), col(SC.publisher))
            )

        return _

    @staticmethod
    def stats_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            return (
                df_stats.transform(StatisticsJobBase.stats_transformation())
                        .where(col(SC.alignment) == AlignmentTypesConst.good)
            )

        return _

    @classmethod
    def get_instance(cls):
        return Bottom10HeroesByAppearancePerPublisherJob()

    @staticmethod
    def auto_run():
        Bottom10HeroesByAppearancePerPublisherJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    Bottom10HeroesByAppearancePerPublisherJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Bottom10HeroesByAppearancePerPublisherJob...")
    main()
    print("[MAIN] - FINISHED!")