from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from cleanUtils.utils import flatten_json, replaceNaNValues
from imports.hdfsIO import HdfsIO

#This class implements the preparation functions for files from the landing zone to the process zone
class Transform:
    def __init__(self, spark):
        self.spark = spark

    def transform_json(self, path, root = None, metafields = None):
        io = HdfsIO(self.spark)
        opt = {"multiLine": True}
        df = io.read(path, 'json', opt)
        norm_df = flatten_json(df, self.spark, root, metafields)
        norm_df = replaceNaNValues(norm_df)
        return norm_df

    def transform_csv(self, path, options):
        io = HdfsIO(self.spark)
        df = io.read(path, 'csv', options)
        return df

    def save(self, df, path, partitionArgs = [], database = None, table = None):
        io = HdfsIO(self.spark)
        io.write(df, path, "parquet", "overwrite", partitionArgs, database, table)
        return    

