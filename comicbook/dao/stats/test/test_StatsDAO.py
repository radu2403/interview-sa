from unittest import mock
from unittest.mock import MagicMock
import pytest

from pyspark.sql import SparkSession, DataFrame
from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.stats.StatsDAO import StatsDAO


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session


@pytest.fixture(scope="module")
def comic_stats_file_path() -> str:
    return abs_path(__file__,"./resources/comic_characters_info.csv")


@pytest.fixture(scope="module")
def config(comic_stats_file_path) -> StatsDAOConfig:
    return StatsDAOConfig(path=comic_stats_file_path)


###############################################################
# -----------------------     TESTS     -----------------------
###############################################################

@pytest.mark.dao
def test_stats_creates_the_class_dao(config: StatsDAOConfig):
    StatsDAO(_config=config)


@pytest.mark.dao
def test_stats_create_load_df(config: StatsDAOConfig):
    # assign
    dao = StatsDAO(_config=config)

    # act
    df = dao.load()

    # assert
    assert df is not None
    assert df.count() > 0

