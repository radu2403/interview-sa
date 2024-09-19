from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, count, lit

from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@dataclass(frozen=True)
class Top10OverallScoreJob(CleanHeroNamesJobTransformation):
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
                    df_ha.transform(self.ha_transformation())
                         .orderBy(col(HC.overall_score).desc())
                         .limit(10)
            )

        return _

    def ha_transformation(self) -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            return (
                df_stats.select(HC.name,
                                HC.overall_score)
                        .transform(self.clean_column_name_transformation(col_name=SC.name))
                        .dropna()
                        .where(col(HC.overall_score) != lit("∞"))         # remove infinite - consider it as an error
                        .withColumn(HC.overall_score, col(HC.overall_score).cast("int"))
                        .dropDuplicates()

            )

        return _

    @classmethod
    def get_instance(cls):
        return Top10OverallScoreJob()

    @staticmethod
    def auto_run():
        Top10OverallScoreJob.get_instance().execute()
        print("[CLASS] - FINISHED!")


def main():
    print(f"[MAIN] - Started main...")
    Top10OverallScoreJob.auto_run()


if __name__ == "__main__":
    print("[MAIN] - Starting the Top10SuperpowersJob...")
    main()
    print("[MAIN] - FINISHED!")