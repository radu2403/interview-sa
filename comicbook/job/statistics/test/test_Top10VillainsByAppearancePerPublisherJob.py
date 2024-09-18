import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, lit

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.marvel.MarvelDAOConfig import MarvelDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.dc.DcDAO import DcDAO
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.marvel.MarvelDAO import MarvelDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.job.statistics.Top10VillainsByAppearancePerPublisherJob import Top10VillainsByAppearancePerPublisherJob
from comicbook.common.conventions.dataset.dc.DcConst import DcConst as DC
from comicbook.common.conventions.dataset.marvel.MarvelConst import MarvelConst as MC
from comicbook.common.conventions.dataset.stats.AlignmentTypesConst import AlignmentTypesConst
from comicbook.common.conventions.dataset.stats.StatsConst import StatsConst as SC


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session

# ------------------------------
# ------ DEFAULT DAOs ----------
# ------------------------------

# ------ STATS DAOs ----------


@pytest.fixture(scope="module")
def comic_stats_file_path() -> str:
    return abs_path(__file__,"../../../dao/stats/test/resources/comic_characters_info.csv")


@pytest.fixture(scope="module")
def stats_config(comic_stats_file_path) -> StatsDAOConfig:
    return StatsDAOConfig(path=comic_stats_file_path)


@pytest.fixture(scope="module")
def stats_dao(stats_config: StatsDAOConfig) -> StatsDAO:
    return StatsDAO(_config=stats_config)


# ------ DC DAOs ----------


@pytest.fixture(scope="module")
def dc_file_path() -> str:
    return abs_path(__file__,"../../../dao/dc/test/resources/dc-data.csv")


@pytest.fixture(scope="module")
def dc_config(dc_file_path) -> DcDAOConfig:
    return DcDAOConfig(path=dc_file_path)


@pytest.fixture(scope="module")
def dc_dao(dc_config: DcDAOConfig) -> DcDAO:
    return DcDAO(_config=dc_config)


# ------ Marvel DAOs ----------


@pytest.fixture(scope="module")
def mv_file_path() -> str:
    return abs_path(__file__,"../../../dao/marvel/test/resources/marvel-data.csv")


@pytest.fixture(scope="module")
def mv_config(mv_file_path) -> MarvelDAOConfig:
    return MarvelDAOConfig(path=mv_file_path)


@pytest.fixture(scope="module")
def marvel_dao(mv_config: MarvelDAOConfig) -> MarvelDAO:
    return MarvelDAO(_config=mv_config)


# ------ Hero Ability DAOs ----------


@pytest.fixture(scope="module")
def ha_file_path() -> str:
    return abs_path(__file__,"../../../dao/heroabilities/test/resources/hero-abilities.csv")


@pytest.fixture(scope="module")
def ha_config(ha_file_path) -> HeroAbilityDAOConfig:
    return HeroAbilityDAOConfig(path=ha_file_path)


@pytest.fixture(scope="module")
def hero_ability_dao(ha_config: HeroAbilityDAOConfig) -> HeroAbilityDAO:
    return HeroAbilityDAO(_config=ha_config)


###############################################################
# -----------------------     TESTS     -----------------------
###############################################################


@pytest.mark.job
def test_that_the_job_is_created_with_success_with_parameters(stats_dao, dc_dao, marvel_dao, hero_ability_dao):
    # act
    Top10VillainsByAppearancePerPublisherJob(
        _stats_dao=stats_dao,
        _dc_dao=dc_dao,
        _marvel_dao=marvel_dao,
        _hero_ability_dao=hero_ability_dao
    )


@pytest.mark.job
def test_that_the_job_is_created_with_success_without_parameters(stats_dao, dc_dao, marvel_dao, hero_ability_dao):
    # act
    Top10VillainsByAppearancePerPublisherJob()


@pytest.mark.job
def test_that_the_job_is_executed_and_return_10_values(stats_dao, dc_dao, marvel_dao):
    # assign
    job = Top10VillainsByAppearancePerPublisherJob(
        _stats_dao=stats_dao,
        _dc_dao=dc_dao,
        _marvel_dao=marvel_dao
    )

    # act
    df = job.execute()

    # assign
    assert df.count() == 10


@pytest.mark.job
def test_that_the_job_mv_trans_return_name_and_appearance(marvel_dao):
    # assign
    mv_df = marvel_dao.load()

    # act
    df = mv_df.transform(Top10VillainsByAppearancePerPublisherJob.marvel_transformation())

    # assign
    assert len(df.columns) == 2


@pytest.mark.job
def test_that_the_job_mv_trans_return_name_and_appearance(dc_dao):
    # assign
    dc_df = dc_dao.load()

    # act
    df = dc_df.transform(Top10VillainsByAppearancePerPublisherJob.dc_transformation())

    # assign
    assert len(df.columns) == 2


@pytest.mark.job
def test_that_the_job_mv_trans_return_name_without_parentheses(dc_dao):
    # assign
    dc_df = dc_dao.load()

    # act
    df = dc_df.transform(Top10VillainsByAppearancePerPublisherJob.dc_transformation())

    # assign
    assert df.where(col(MC.name).contains("(")).count() == 0


@pytest.mark.job
def test_that_the_job_stats_trans_return_name_without_parentheses(stats_dao):
    # assign
    st_df = stats_dao.load()

    # act
    df = st_df.transform(Top10VillainsByAppearancePerPublisherJob.stats_transformation())

    # assign
    assert df.where(col(SC.name).contains("(")).count() == 0


@pytest.mark.job
def test_that_the_job_stats_trans_return_names_of_bad_actors(stats_dao):
    # assign
    st_df = stats_dao.load()

    # act
    df = st_df.transform(Top10VillainsByAppearancePerPublisherJob.stats_transformation())

    # assign
    assert st_df.where(lower(col(SC.alignment)) == lit(AlignmentTypesConst.bad)).count() == df.count()


@pytest.mark.job
def test_that_the_job_transformation_is_ordered_desc(stats_dao, dc_dao, marvel_dao):
    # assign
    job = Top10VillainsByAppearancePerPublisherJob(
        _stats_dao=stats_dao,
        _dc_dao=dc_dao,
        _marvel_dao=marvel_dao
    )

    # act
    df = job.execute()
    res = df.collect()

    # assign
    assert res[0][MC.appearances] >= res[1][MC.appearances]
    assert res[2][MC.appearances] >= res[3][MC.appearances]


@pytest.mark.job
def test_that_the_job_transformation_not_duplicates(stats_dao, dc_dao, marvel_dao):
    # assign
    job = Top10VillainsByAppearancePerPublisherJob(
        _stats_dao=stats_dao,
        _dc_dao=dc_dao,
        _marvel_dao=marvel_dao
    )

    # act
    df = job.execute()

    # assign
    assert df.count() == df.dropDuplicates().count()