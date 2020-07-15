
# Artificial normal distribution datasets generating
import random
import string
import pandas as pd 
def generator(quantity, minlat, maxlat, minlon, maxlon):
    points = []
    i = 0
    while i<quantity:
        lat = round(random.uniform(minlat,maxlat), 6)
        lon = round(random.uniform(minlon,maxlon), 6)
        nam = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

        points.append((nam,lat,lon))
        i += 1
    return points
    #print(points)
uniformA = generator(1000, 0, 80, 0, 80)
uniformB = generator(1000, 0, 80, 0, 80)
points = uniformA
points2 = uniformB


# imports for spark and local clustering
import findspark
findspark.init()
import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession

from pyspark import SparkContext
from pyspark.conf import SparkConf

conf = SparkConf()
conf.setMaster('spark://192.168.56.1:7077').setAppName('ipython-notebook').set("spark.executor.memory", "7g")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

import pandas as pd
#sc = spark.sparkContext
import random
import string
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, DoubleType, ArrayType, StructField, StructType, StringType, IntegerType, FloatType
pd.set_option('display.max_rows', 10)
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce 
from pyspark.sql import DataFrame
import time


# create schema for spark dataframe for the first dataset
schema = StructType([
    StructField('name', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lon', FloatType(), True)
])

# convert python list to RDD
rdd = spark.sparkContext.parallelize(points)

# create spark data frame
df6 = spark.createDataFrame(rdd,schema)


# create schema for spark dataframe for the second dataset
schema2 = StructType([
    StructField('name', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lon', FloatType(), True)
])

# convert python list to RDD
rdd2 = spark.sparkContext.parallelize(points2)

# create spark data frame
df7 = spark.createDataFrame(rdd2,schema2)



# wanted distance
theta = 5

# wanted partitions number
partitions_n = 4

# wanted dataset percentage to take as a sample
dataset_fracture = 0.9

# wanted error that you are willing to take. High value of error offers faster times but lower accuracy. Low value offers reversed results
relative_error = 0.1


# we give an index to each set

df6 = df6.withColumn("set0",lit("A"))
df7 = df7.withColumn("set0",lit("B"))

# union of the two datasets
df8 = df6.union(df7)


# sampling
sample_df = df8.sample(dataset_fracture)

# finding of quantiles based on latitude
qlist = []
for i in range(1, partitions_n):
    q = sample_df.approxQuantile("lat", [i/partitions_n], relative_error)
    percentile = q[0]
    qlist.append(percentile)
    


print(qlist)


# comparing lat of each record to the latitiudes that have been defined as boundaries.
# each record gets an attachement of the spatial zone where it belongs to

# the process is automated so as to work for everey wanted partitions number
for i in range(partitions_n):
    if i ==0:
        df8=df8.withColumn("zone_id",
                        when( (col('lat') < qlist[i]), i))
    if i > 0 and i < partitions_n - 1:
        df8=df8.withColumn("zone_id",
                        when( col("zone_id").isNull() & (col('lat') >= qlist[i-1]) & (col('lat') < qlist[i]), i)
                        .otherwise(col("zone_id")))
    if i == partitions_n - 1:
        df8=df8.withColumn("zone_id",
                        when( col("zone_id").isNull(), i)
                        .otherwise(col("zone_id")))
        
df10=df8

# convert distance to degrees
diff1 = theta / 110.567

# importing of columns with boundaries that if "lat+theta" goes past them, then that record should be copied for a new ZONE
df11 = df10.withColumn("sum_plus", col("lat")+diff1)
df11 = df11.withColumn("sum_minus", col("lat")-diff1)

# temporary sql table based on our dataframe
df11.createOrReplaceTempView("table10_0")

# spark sql queries so as to find which records should be copied. The process is fully automated so as to be able to work for every amount of partitions, and so for every quantiles that we may meet
# creating of a python dictionary. KEY is the zone, VALUE are the new records that will be copied to each zone

extra_dfs = {}    
for i in range(partitions_n):
    if i == 0:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id > 0 AND sum_minus < {} ) ".format(qlist[0]))
    
    if i >0 and i<partitions_n-1:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id < {} AND sum_plus > {} ) OR ( set0 = 'B' AND zone_id > {} AND sum_minus < {} ) ".format(i, qlist[i-1], i, qlist[i]))
        
    if i == partitions_n -1:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id < {} AND sum_plus > {} ) ".format(i, qlist[i-1]))
        

# drop columns that we don't need any more
df11 = df11.drop('sum_plus', 'sum_minus')

# for each "extra records", we drop the columns that we don't need and we import a column with the indec of the zone that they now belong to.
j=0
extra_dfs_final_dict = {}
for key, value in extra_dfs.items():
    extra_dfs_final_dict["exs{0}".format(j)] = extra_dfs[key].drop('sum_plus', 'sum_minus').withColumn("zone_id",lit(j))
    j+=1

    
# union of everything
dfs = []
dfs.append(df11)
for key, value in extra_dfs_final_dict.items():
    dfs.append(extra_dfs_final_dict[key])
    
dff = reduce(DataFrame.unionAll, dfs)    

dff.cache()



no_of_partitions = partitions_n
#no_of_partitions = partitions_n*6

# repartition of the data, based on zone index
dffj1 = dff.repartition(no_of_partitions, col("zone_id"))

#  sql table
dffj1.createOrReplaceTempView("dffj")

# creating of dataframes, one for each category of data
df_A = spark.sql("SELECT * FROM dffj where set0 = 'A'")
df_B = spark.sql("SELECT * FROM dffj where set0 = 'B'")



df_A.cache()
df_B.cache()



#print("Number of partitions: {}".format(dffj1.rdd.getNumPartitions())) 
# checking of the records amount in each partition
print('Partitioning distribution: '+ str(dffj1.rdd.glom().map(len).collect()))


# cross join between the two tables. we are searching for pairs in a distance below theta
def distance(df1, df2):
    df1.createOrReplaceTempView("table1")
    df2.createOrReplaceTempView("table2")
    dfjoin = spark.sql("SELECT table1.name, table2.name from table1 cross join table2 where (acos(sin(radians(table1.lat)) * sin(radians(table2.lat)) + cos(radians(table1.lat)) * cos(radians(table2.lat)) * cos(radians(table1.lon - table2.lon))) * 6372.8)< {} ".format(theta))
    #return dfjoin.show()

start_time = time.time()

distance(df_A, df_B)

end_time = time.time()
print("{} seconds".format(end_time - start_time))

