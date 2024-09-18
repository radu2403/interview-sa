from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from comicbook.common.conventions.dataset.stats.AlignmentTypesConst import AlignmentTypesConst
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.common.conventions.dataset.marvel.MarvelConst import MarvelConst as MC
from comicbook.job.statistics.StatisticsJobBase import StatisticsJobBase


@dataclass(frozen=True)
class Top10VillainsByAppearancePerPublisherJob(StatisticsJobBase):

    def transformation(self, df_marvel: DataFrame, df_dc: DataFrame) -> Callable[[DataFrame], DataFrame]:
        super_tr = super().transformation(df_marvel, df_dc)

        def _(df_stats: DataFrame) -> DataFrame:

            return (
                df_stats.transform(super_tr)
                        .orderBy(col(MC.appearances).desc())
                        .limit(10)
            )

        return _

    @staticmethod
    def stats_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            return (
                df_stats.transform(StatisticsJobBase.stats_transformation())
                        .where(col(SC.alignment) == AlignmentTypesConst.bad)
            )

        return _

    @classmethod
    def get_instance(cls):
        return Top10VillainsByAppearancePerPublisherJob()

    @staticmethod
    def auto_run():
        Top10VillainsByAppearancePerPublisherJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    Top10VillainsByAppearancePerPublisherJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Top10VillainsByAppearancePerPublisherJob...")
    main()
    print("[MAIN] - FINISHED!")