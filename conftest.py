from pyspark.sql import SparkSession
import pytest
from shutil import rmtree
from os import remove, path


@pytest.fixture(scope="session")
def test_root_dir():
    root_dir = path.dirname(path.abspath(__file__))
    return root_dir


@pytest.fixture(scope="session")
def hive_dir(tmpdir_factory):
    hive_dir = tmpdir_factory.mktemp("hive", numbered=False)
    print("Created directory for hive at", hive_dir)
    yield str(hive_dir.dirpath())
    hive_dir.remove(ignore_errors=True)


@pytest.fixture(scope="session")
def spark_session(hive_dir, tmpdir_factory):
    spark_session = (
        SparkSession.builder.appName("Comic-Book - Unit test spark")
        .master("local[*]")
        .getOrCreate()
    )
    spark_session.sql("set spark.sql.datetime.java8API.enabled = true")
    spark_session.sql("SET TIME ZONE '+00:00'")
    spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark_session
    spark_session.stop()
    # Need to cleanup the metastore_db and derby.log after the tests finished.
    # TODO: Find out how to store_traffic them in a tmp file
    root_dir = path.dirname(path.abspath(__file__))
    derby_log_file = root_dir + "/derby.log"
    if path.exists(derby_log_file):
        remove(derby_log_file)
    metastore_db_dir = root_dir + "/metastore_db"
    rmtree(metastore_db_dir, ignore_errors=True)