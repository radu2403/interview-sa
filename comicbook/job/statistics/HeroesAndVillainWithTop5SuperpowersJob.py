from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, count, collect_set, broadcast

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@dataclass(frozen=True)
class HeroesAndVillainWithTop5SuperpowersJob(CleanHeroNamesJobTransformation):
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
            df_ha_tr = df_ha.transform(self.ha_transformation()).cache()

            top5_superpowers_df = (df_ha_tr.transform(self.ha_top_5_superpowers()))
            return (
                    df_ha_tr.join(broadcast(top5_superpowers_df),
                               on=[HC.superpowers],
                               how="inner")
                            .groupBy(HC.name)
                            .agg(collect_set(HC.superpowers).alias("superpowers"))
            )

        return _

    def ha_top_5_superpowers(self) -> Callable[[DataFrame], DataFrame]:
        def _(df_ha: DataFrame) -> DataFrame:
            return (df_ha.groupBy(HC.superpowers)
                         .agg(count(HC.superpowers).alias("count"))
                         .orderBy(col("count").desc())
                         .limit(5)
                         .drop("count")
                    )

        return _

    def ha_transformation(self) -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            return (
                df_stats.select(HC.name,
                                HC.superpowers)
                        .transform(self.clean_column_name_transformation(col_name=SC.name))
                        .dropDuplicates()
                .withColumn(HC.superpowers, explode(HC.superpowers))
            )

        return _

    @classmethod
    def get_instance(cls):
        return HeroesAndVillainWithTop5SuperpowersJob()

    @staticmethod
    def auto_run():
        HeroesAndVillainWithTop5SuperpowersJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    HeroesAndVillainWithTop5SuperpowersJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Top10SuperpowersJob...")
    main()
    print("[MAIN] - FINISHED!")