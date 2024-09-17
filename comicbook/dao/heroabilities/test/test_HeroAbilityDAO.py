import pytest
from pyspark.sql import SparkSession

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session


@pytest.fixture(scope="module")
def hero_abilities_file_path() -> str:
    return abs_path(__file__,"./resources/hero-abilities.csv")


@pytest.fixture(scope="module")
def config(hero_abilities_file_path) -> HeroAbilityDAOConfig:
    return HeroAbilityDAOConfig(path=hero_abilities_file_path)


###############################################################
# -----------------------     TESTS     -----------------------
###############################################################

@pytest.mark.dao
def test_stats_creates_the_class_dao(config: HeroAbilityDAOConfig):
    HeroAbilityDAO(_config=config)


@pytest.mark.dao
def test_stats_create_load_df(config: HeroAbilityDAOConfig):
    # assign
    dao = HeroAbilityDAO(_config=config)

    # act
    df = dao.load()

    # assert
    assert df is not None
    assert df.count() > 0

