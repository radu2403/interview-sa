import pytest
from pyspark.sql import SparkSession

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.marvel.MarvelDAOConfig import MarvelDAOConfig
from comicbook.dao.marvel.MarvelDAO import MarvelDAO


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session


@pytest.fixture(scope="module")
def marvel_file_path() -> str:
    return abs_path(__file__,"./resources/marvel-data.csv")


@pytest.fixture(scope="module")
def config(marvel_file_path) -> MarvelDAOConfig:
    return MarvelDAOConfig(path=marvel_file_path)


###############################################################
# -----------------------     TESTS     -----------------------
###############################################################

@pytest.mark.dao
def test_stats_creates_the_class_dao(config: MarvelDAOConfig):
    MarvelDAO(_config=config)


@pytest.mark.dao
def test_stats_create_load_df(config: MarvelDAOConfig):
    # assign
    dao = MarvelDAO(_config=config)

    # act
    df = dao.load()

    # assert
    assert df is not None
    assert df.count() > 0

