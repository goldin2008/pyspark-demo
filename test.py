import sys
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

# import pytest

# from pysparktestingexample.functions import remove_non_word_characters
# from chispa import *
# import pyspark.sql.functions as F

# def test_remove_non_word_characters(spark):
#     data = [
#         ("jo&&se", "jose"),
#         ("**li**", "li"),
#         ("#::luisa", "luisa"),
#         (None, None)
#     ]
#     df = spark.createDataFrame(data, ["name", "expected_name"])\
#         .withColumn("clean_name", remove_non_word_characters(F.col("name")))
#     # assert_column_equality(df, "clean_name", "expected_name")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName('test_spark_session') \
#         .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1) \
#         .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1).getOrCreate()
#     spark.sparkContext.setLogLevel('ERROR')

spark = SparkSession.builder.appName("Datacamp Pyspark Tutorial").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()
df = spark.read.csv('data/online_retail.csv',header=True,escape="\"")
df.show(5,0)
df.count()  # Answer: 2,500
df.select('CustomerID').distinct().count() # Answer: 95

df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).show()

df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).orderBy(desc('country_count')).show()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn('date',to_timestamp("InvoiceDate", 'yy/MM/dd HH:mm'))
df.select(max("date")).show()

df.select(min("date")).show()

df.show(5,0)

df = df.withColumn("from_date", lit("12/1/10 08:26"))
df = df.withColumn('from_date',to_timestamp("from_date", 'yy/MM/dd HH:mm'))

df2=df.withColumn('from_date',to_timestamp(col('from_date'))).withColumn('recency',col("date").cast("long") - col('from_date').cast("long"))
df2 = df2.join(df2.groupBy('CustomerID').agg(max('recency').alias('recency')),on='recency',how='leftsemi')
df2.show(5,0)
df2.printSchema()


df_freq = df2.groupBy('CustomerID').agg(count('InvoiceDate').alias('frequency'))
df_freq.show(5,0)
df3 = df2.join(df_freq,on='CustomerID',how='inner')
df3.printSchema()


m_val = df3.withColumn('TotalAmount',col("Quantity") * col("UnitPrice"))
m_val = m_val.groupBy('CustomerID').agg(sum('TotalAmount').alias('monetary_value'))
finaldf = m_val.join(df3,on='CustomerID',how='inner')
finaldf = finaldf.select(['recency','frequency','monetary_value','CustomerID']).distinct()



assemble=VectorAssembler(inputCols=[
    'recency','frequency','monetary_value'
], outputCol='features')

assembled_data=assemble.transform(finaldf)

scale=StandardScaler(inputCol='features',outputCol='standardized')
data_scale=scale.fit(assembled_data)
data_scale_output=data_scale.transform(assembled_data)

data_scale_output.select('standardized').show(2,truncate=False)


from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np

cost = np.zeros(10)

evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized',metricName='silhouette', distanceMeasure='squaredEuclidean')

for i in range(2,10):
    KMeans_algo=KMeans(featuresCol='standardized', k=i)
    KMeans_fit=KMeans_algo.fit(data_scale_output)
    output=KMeans_fit.transform(data_scale_output)
    cost[i] = KMeans_fit.summary.trainingCost


import pandas as pd
import pylab as pl
df_cost = pd.DataFrame(cost[2:])
df_cost.columns = ["cost"]
new_col = range(2,10)
df_cost.insert(0, 'cluster', new_col)
pl.plot(df_cost.cluster, df_cost.cost)
pl.xlabel('Number of Clusters')
pl.ylabel('Score')
pl.title('Elbow Curve')
# pl.show()
pl.savefig("figure/Elbow Curve.png")

KMeans_algo=KMeans(featuresCol='standardized', k=4)
KMeans_fit=KMeans_algo.fit(data_scale_output)

preds=KMeans_fit.transform(data_scale_output)

preds.show(5,0)

import matplotlib.pyplot as plt
import seaborn as sns

df_viz = preds.select('recency','frequency','monetary_value','prediction')
df_viz = df_viz.toPandas()
avg_df = df_viz.groupby(['prediction'], as_index=False).mean()

list1 = ['recency','frequency','monetary_value']

for i in list1:
    sns.barplot(x='prediction',y=str(i),data=avg_df)
    # plt.show()
    pl.savefig("figure/"+i+".png")
