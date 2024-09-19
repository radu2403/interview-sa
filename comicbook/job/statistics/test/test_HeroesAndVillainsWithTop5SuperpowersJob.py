import pytest
from pyspark.sql import SparkSession

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.marvel.MarvelDAOConfig import MarvelDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.dc.DcDAO import DcDAO
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.marvel.MarvelDAO import MarvelDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.job.statistics.HeroesAndVillainWithTop5SuperpowersJob import HeroesAndVillainWithTop5SuperpowersJob


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
def test_that_the_job_is_created_with_success_with_parameters(stats_dao, hero_ability_dao):
    # act
    HeroesAndVillainWithTop5SuperpowersJob(
        _heroes_ability_dao=hero_ability_dao
    )


@pytest.mark.job
def test_that_the_job_is_created_with_success_without_parameters():
    # act
    HeroesAndVillainWithTop5SuperpowersJob()


@pytest.mark.job
def test_that_the_job_is_executed_and_return_10_values(stats_dao, hero_ability_dao):
    # assign
    job = HeroesAndVillainWithTop5SuperpowersJob(
        _heroes_ability_dao=hero_ability_dao
    )

    # act
    df = job.execute()
    df.show(10000, truncate=False)

    # assign
    assert df.count() > 0





