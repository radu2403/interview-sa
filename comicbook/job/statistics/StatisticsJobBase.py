from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, sum

from comicbook.common.conventions.dataset.dc.DcConst import DcConst as DC
from comicbook.common.conventions.dataset.marvel.MarvelConst import MarvelConst as MC
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC
from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.conf.dao.marvel.MarvelDAOConfig import MarvelDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.dc.DcDAO import DcDAO
from comicbook.dao.marvel.MarvelDAO import MarvelDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@dataclass(frozen=True)
class StatisticsJobBase(CleanHeroNamesJobTransformation):
    _stats_dao: StatsDAO = field(repr=False, default=None)
    _marvel_dao: MarvelDAO = field(repr=False, default=None)
    _dc_dao: DcDAO = field(repr=False, default=None)

    def __post_init__(self):
        if not self._stats_dao or not self._marvel_dao or not self._dc_dao:
            if not self._stats_dao:
                config = StatsDAOConfig()
                object.__setattr__(self, "_stats_dao", StatsDAO(_config=config))

            if not self._marvel_dao:
                config = MarvelDAOConfig()
                object.__setattr__(self, "_marvel_dao", MarvelDAO(_config=config))

            if not self._dc_dao:
                config = DcDAOConfig()
                object.__setattr__(self, "_dc_dao", DcDAO(_config=config))

    def execute(self) -> DataFrame:
        # load
        df_stats = self._stats_dao.load()
        df_marvel = self._marvel_dao.load()
        df_dc = self._dc_dao.load()

        # transform
        df = df_stats.transform(self.transformation(df_marvel, df_dc))

        # sink
        return df

    def transformation(self, df_marvel: DataFrame, df_dc: DataFrame) -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            df_stats_tr = df_stats.transform(self.stats_transformation())

            df_marvel_tr = df_marvel.transform(self.marvel_transformation())
            df_dc_tr = df_dc.transform(self.dc_transformation())

            union_df = (df_marvel_tr.unionByName(df_dc_tr)
                                   .groupby(MC.name)
                                   .agg(sum(MC.appearances).alias(MC.appearances))
                        )
            return (
                    df_stats_tr.join(union_df,
                                     on=[SC.name],
                                     how="inner")
            )

        return _

    @staticmethod
    def stats_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_stats: DataFrame) -> DataFrame:
            this = StatisticsJobBase

            return (
                df_stats.select(SC.name,
                                SC.alignment,
                                SC.publisher)
                        .transform(this.clean_column_name_transformation(col_name=SC.name))
                        .withColumn(SC.alignment, lower(col(SC.alignment)))
                        .dropDuplicates()

            )

        return _

    @staticmethod
    def marvel_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_marvel: DataFrame) -> DataFrame:
            this = StatisticsJobBase

            return (
                    df_marvel.select(MC.name,
                                     MC.appearances)
                             .transform(this.clean_column_name_transformation(col_name=MC.name))
                             .dropDuplicates()
            )

        return _

    @staticmethod
    def dc_transformation() -> Callable[[DataFrame], DataFrame]:
        def _(df_dc: DataFrame) -> DataFrame:
            this = StatisticsJobBase

            return (
                    df_dc.select(DC.name,
                                 DC.appearances)
                         .transform(this.clean_column_name_transformation(DC.name))
                         .dropDuplicates()

            )

        return _

