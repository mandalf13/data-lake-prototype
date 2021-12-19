from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pandas as pd
from pyspark.sql.types import ArrayType, DateType, StringType
from pyspark.sql.functions import isnan, when, count, col
import json

#This function infer the schema of a dataframe
def castDataFrame(df, spark):
    df1 = df.toPandas()
    df1 = df1.replace('NA', pd.NA)
    df1 = df1.replace('NaN', pd.NA)
    for c in df1.columns:
        try:
            df1[c] = pd.to_numeric(df1[c])
        except:
            try:
                df1[c]=df1[c].astype('datetime64[ns]')
            except:
                pass
    return spark.createDataFrame(df1)   

#This function execute the flattening algorithm of pandas (json_normalize) over document oriented datasets
def flatten_json(df, spark, root=None, metafields=None, separator = '_'):
    rdd = df.toJSON()
    rdd_data = rdd.collect()
    data = []
    for elem in rdd_data:
        data.append(json.loads(elem))
    normalized_df = pd.json_normalize(data, root, metafields, sep=separator)
    return spark.createDataFrame(normalized_df)              

#This function replace NaN values for None
def replaceNaNValues(df):
    df1 = df
    schema = df1.schema
    for c in df1.columns:
        t = type(schema[c].dataType)
        if t != ArrayType and t != DateType and t != StringType:
            df1 = df1.withColumn(c, when(isnan(col(c)),None).otherwise(col(c)))
    return df1

#This function compute the min and max bounds for outlier clasification based on the boxplot method
def compute_bounds(df):
    df_types = df.dtypes #return a list of tuples, each tuple contain a column name and type
    bounds = {}
    for t in df_types:
        if t[1] == 'int' or t[1] == 'double' or t[1] == 'float' or t[1] == 'decimal' or t[1] == 'long':
            t_quantiles = df.approxQuantile(t[0], [0.25, 0.75], 0)
            bounds[t[0]] = {'q1': t_quantiles[0], 'q3': t_quantiles[1]}
    for c in bounds:
        iqr = bounds[c]['q3'] - bounds[c]['q1']
        bounds[c]['min'] = bounds[c]['q1'] - (iqr * 1.5)
        bounds[c]['max'] = bounds[c]['q3'] + (iqr * 1.5)
    return bounds    

#return the percentage of outliers in a given column c of a dataframe
def outliers_percentage(df, c, bounds): 
    df1 = df.select(col(c))
    df1 = df1.dropna()
    total = df1.count()
    outliers = df1.filter((col(c) < bounds[c]['min']) | (col(c) > bounds[c]['max']))
    return round((outliers.count() * 100) / total, 2)

#return a list of tuples, each tuple with column name and count of missing values 
def get_null_values(df):
    df1 = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    l = df1.collect()[0]
    cols = df1.columns
    result = []
    for i in range(0,len(cols)):
        result.append((cols[i],l[i]))
        #print(cols[i] + ": " + str(l[i]))
    return result

#return the percentage of missing values in the dataframe
def get_completeness_percentage(df):
    l = get_null_values(df)
    s = sum([t[1] for t in l])
    total = df.count() * len(l)
    return round((s * 100)/total,2)
