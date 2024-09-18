import pytest
from pyspark.sql import SparkSession

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.dao.dc.DcDAO import DcDAO


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session


@pytest.fixture(scope="module")
def dc_file_path() -> str:
    return abs_path(__file__, "../../dc/test/resources/dc-data.csv")


@pytest.fixture(scope="module")
def config(dc_file_path) -> DcDAOConfig:
    return DcDAOConfig(path=dc_file_path)


###############################################################
# -----------------------     TESTS     -----------------------
###############################################################


@pytest.mark.dao
def test_stats_creates_the_class_dao(config: DcDAOConfig):
    DcDAO(_config=config)


@pytest.mark.dao
def test_stats_create_load_df(config: DcDAOConfig):
    # assign
    dao = DcDAO(_config=config)

    # act
    df = dao.load()

    # assert
    assert df is not None
    assert df.count() > 0

