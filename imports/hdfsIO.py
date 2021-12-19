from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

class HdfsIO:
    def __init__(self, spark):
        self.spark = spark

    def read(self, location, filetype, opt={}, table=None):
        try:
            if str(filetype).lower().__eq__('tbl'):
                return self.spark.read.table(table)
            elif str(filetype).lower().__eq__('csv'):
                return self.spark.read.options(**opt).csv(location)
            elif str(filetype).lower().__eq__('json'):
                return self.spark.read.options(**opt).json(location)
            elif str(filetype).lower().__eq__('orc'):
                return self.spark.read.options(**opt).orc(location)
            elif str(filetype).lower().__eq__('parquet'):
                return self.spark.read.options(**opt).parquet(location)
            elif str(filetype).lower().__eq__('avro'):
                return self.spark.read.options(**opt).avro(location)    
            else:
                raise "Invalid filetype: " + filetype
        except Exception as ex:
            print("Error reading file in Spark of filetype " + filetype + " Error details: " + str(ex))
    
    def write(self, dataframe, location, filetype, mode, partitionArgs = [], database = None, table = None):
        try:
            if database != None and table != None:
                ddl = "USE " + database
                self.spark.sql(ddl)
                dataframe.write.format(filetype)\
                    .partitionBy(*partitionArgs)\
                    .mode(mode)\
                    .option("path", location)\
                    .saveAsTable(table)
            else:
                dataframe.write.format(filetype)\
                    .partitionBy(*partitionArgs)\
                    .mode(mode)\
                    .save(location)
        except Exception as ex:
            print("Error writing file in Spark of filetype " + filetype + " Error details: " + str(ex))
        return    