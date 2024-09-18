from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, count

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@dataclass(frozen=True)
class Top5SuperpowersJob(CleanHeroNamesJobTransformation):
    _heroes_ability_dao: HeroAbilityDAO = field(repr=False, default=None)

    def __post_init__(self):
        if not self._heroes_ability_dao:
            if not self._heroes_ability_dao:
                config = HeroAbilityDAOConfig()
                object.__setattr__(self, "_stats_dao", HeroAbilityDAO(_config=config))

    def execute(self) -> DataFrame:
        # load
        df_ha = self._heroes_ability_dao.load()

        # transform
        df = df_ha.transform(self.transformation())

        # sink
        return df

    def transformation(self) -> Callable[[DataFrame], DataFrame]:
        def _(df_ha: DataFrame) -> DataFrame:

            return (
                    df_ha.withColumn(HC.superpowers, explode(HC.superpowers))
                         .groupBy(HC.superpowers)
                         .agg(count(HC.superpowers).alias("count"))
                         .orderBy(col("count").desc())
                         .limit(10)
            )

        return _

    @staticmethod
    def ha_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            this = Top5SuperpowersJob
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
            this = Top5SuperpowersJob

            return (
                df_stats.select(SC.name,
                                SC.publisher)
                        .transform(this.clean_column_name_transformation(col_name=SC.name))
                        .dropDuplicates()

            )

        return _

    @classmethod
    def get_instance(cls):
        return Top5SuperpowersJob()

    @staticmethod
    def auto_run():
        Top5SuperpowersJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    Top5SuperpowersJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Top10SuperpowersJob...")
    main()
    print("[MAIN] - FINISHED!")