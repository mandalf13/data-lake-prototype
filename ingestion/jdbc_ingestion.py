from pyspark.sql import SparkSession

class JdbcConnector:
    def __init__(self, spark, user, password, database, url, driver):
        self.spark = spark
        self.user = user
        self.password = password
        self.database = database
        self.url = url
        self.driver = driver

    def readTable(self, table):
        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
        return jdbcDF    