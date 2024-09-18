from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, explode, count, row_number

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@dataclass(frozen=True)
class Top10SuperpowersPerPublisherJob(CleanHeroNamesJobTransformation):
    _heroes_ability_dao: HeroAbilityDAO = field(repr=False, default=None)
    _stats_dao: StatsDAO = field(repr=False, default=None)

    def __post_init__(self):
        if not self._heroes_ability_dao or not self._stats_dao:
            if not self._heroes_ability_dao:
                config = HeroAbilityDAOConfig()
                object.__setattr__(self, "_stats_dao", HeroAbilityDAO(_config=config))

            if not self._stats_dao:
                config = StatsDAOConfig()
                object.__setattr__(self, "_stats_dao", StatsDAO(_config=config))

    def execute(self) -> DataFrame:
        # load
        df_ha = self._heroes_ability_dao.load()
        df_stats = self._stats_dao.load()

        # transform
        df = df_ha.transform(self.transformation(df_stats))

        # sink
        return df

    def transformation(self, df_stats: DataFrame) -> Callable[[DataFrame], DataFrame]:
        def _(df_ha: DataFrame) -> DataFrame:
            df_ha_tr = df_ha.transform(self.ha_transformation())
            df_stats_tr = df_stats.transform(self.stats_transformation())

            w = Window.partitionBy(SC.publisher).orderBy(col("count").desc())

            return (
                    df_ha_tr.join(df_stats_tr,
                                  on=[HC.name],
                                  how="inner")
                            .withColumn(HC.superpowers, explode(HC.superpowers))
                            .groupBy(HC.superpowers, SC.publisher)
                            .agg(count(HC.superpowers).alias("count"))
                            .withColumn("id", row_number().over(w))
                            .where(col("id") <= 10)
            )

        return _

    @staticmethod
    def ha_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            this = Top10SuperpowersPerPublisherJob
            return (
                df_stats.select(HC.name,
                                HC.superpowers)
                        .transform(this.clean_column_name_transformation(col_name=SC.name))
                        .dropDuplicates()

            )

        return _

    @staticmethod
    def stats_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            this = Top10SuperpowersPerPublisherJob

            return (
                df_stats.select(SC.name,
                                SC.publisher)
                        .transform(this.clean_column_name_transformation(col_name=SC.name))
                        .dropDuplicates()

            )

        return _

    @classmethod
    def get_instance(cls):
        return Top10SuperpowersPerPublisherJob()

    @staticmethod
    def auto_run():
        Top10SuperpowersPerPublisherJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    Top10SuperpowersPerPublisherJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Top10SuperpowersPerPublisherJob...")
    main()
    print("[MAIN] - FINISHED!")