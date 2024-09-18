import pytest
from pyspark.sql import SparkSession

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.common.conventions.dataset.heroability.HeroAbilityConst import HeroAbilityConst as HC


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

def test_df_result_superpowers_col_is_array(config: HeroAbilityDAOConfig):
    # assign
    dao = HeroAbilityDAO(_config=config)

    # act
    df = dao.load()

    # assert
    superpower = df.select(HC.superpowers).where(f"{HC.superpowers} IS NOT NULL").take(1)[0][HC.superpowers]
    assert type(superpower) == list

