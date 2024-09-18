import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from comicbook.common.mocks.test_utils import abs_path
from comicbook.conf.dao.dc.DcDAOConfig import DcDAOConfig
from comicbook.conf.dao.heroability.HeroAbilityDAOConfig import HeroAbilityDAOConfig
from comicbook.conf.dao.marvel.MarvelDAOConfig import MarvelDAOConfig
from comicbook.conf.dao.stats.StatsDAOConfig import StatsDAOConfig
from comicbook.dao.dc.DcDAO import DcDAO
from comicbook.common.conventions.dataset.dc.DcConst import DcConst as DC
from comicbook.dao.heroabilities.HeroAbilityDAO import HeroAbilityDAO
from comicbook.dao.marvel.MarvelDAO import MarvelDAO
from comicbook.dao.stats.StatsDAO import StatsDAO
from comicbook.job.statistics.Top10VillainsByAppearancePerPublisherJob import Top10VillainsByAppearancePerPublisherJob
from comicbook.transformations.CleanHeroNamesJobTransformation import CleanHeroNamesJobTransformation


@pytest.fixture(scope="module")
def spark(spark_session: SparkSession):
    return spark_session


@pytest.fixture(scope="module")
def df(spark):
    data = [("This is a sample (remove this part)",),
            ("Keep this text (and remove this) and this too",),
            ("No parentheses here",)]

    df = spark.createDataFrame(data, ["text"])
    return df

###############################################################
# -----------------------     TESTS     -----------------------
###############################################################


@pytest.mark.transformation
def test_transformation_removes_words_in_parentheses(df):
    # act
    res_df = df.transform(CleanHeroNamesJobTransformation.clean_column_name_transformation(col_name="text"))

    # assert
    assert res_df.where(col("text").contains('(')).count() == 0


@pytest.mark.transformation
def test_transformation_is_trimmed(df):
    # act
    res_df = df.transform(CleanHeroNamesJobTransformation.clean_column_name_transformation(col_name="text"))
    res_df.show(truncate=False)

    # assert
    assert res_df.where(col("text").endswith(' ')).count() == 0


@pytest.mark.transformation
def test_transformation_doesnt_have_multiple_white_spaces(df):
    # act
    res_df = df.transform(CleanHeroNamesJobTransformation.clean_column_name_transformation(col_name="text"))
    res_df.show(truncate=False)

    # assert
    assert res_df.where(col("text").contains('  ')).count() == 0