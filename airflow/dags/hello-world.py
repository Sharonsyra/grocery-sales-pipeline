from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Hello World").getOrCreate()
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j

LOGGER = log4jLogger.LogManager.getLogger(__name__)


LOGGER.info("Running Application")

LOGGER.info(f"Hello dag")
