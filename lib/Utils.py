from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .enableHiveSupport() \
            .config("spark.logConf", "true") \
            .getOrCreate()


def get_spark_session(env):
    if env == "LOCAL":
        return spark,spark.sparkContext.setLogLevel("INFO")




    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()
