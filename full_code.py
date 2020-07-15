
# Δημιουργία συνθετικών συνόλων δεδομένων ομοιόμορφης κατανομής
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


# imports για spark και cluster
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


# Δημιουργία schema για spark dataframe για το πρώτο σύνολο
schema = StructType([
    StructField('name', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lon', FloatType(), True)
])

# Μετατροπή python list σε RDD
rdd = spark.sparkContext.parallelize(points)

# δημιουργία spark data frame
df6 = spark.createDataFrame(rdd,schema)


# Δημιουργία schema για spark dataframe για το δεύτερο σύνολο
schema2 = StructType([
    StructField('name', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lon', FloatType(), True)
])

# Μετατροπή python list σε RDD
rdd2 = spark.sparkContext.parallelize(points2)

# δημιουργία spark data frame
df7 = spark.createDataFrame(rdd2,schema2)



# επιθυμητο "θ"
theta = 5

# επιθυμητός αριθμός partitions
partitions_n = 4

# επιθυμητό ποσοστό των δεδομένων , το οποόι θα πάρουμε ως δείγμα
dataset_fracture = 0.9

# επιθυμη΄τό σφάλμα κατά των υπολογισμό ποσοστημορίων. Υψηλό σφάλμα φέρνει γρηγορότερο χρόνο και χαμηλή ακρίβεια. Χαμηλο σφάλμα φέρνει τα αντίστροφα
relative_error = 0.1


# κάθε σετ πα΄ίρνει μια ένδειξη

df6 = df6.withColumn("set0",lit("A"))
df7 = df7.withColumn("set0",lit("B"))

# ένωση των δυο σετ σε ένα
df8 = df6.union(df7)



# δείγμα από τα δεδομένα
sample_df = df8.sample(dataset_fracture)

# εξεύρεση προσεγγιστικών ποσοστημορίων ανα latitude
qlist = []
for i in range(1, partitions_n):
    q = sample_df.approxQuantile("lat", [i/partitions_n], relative_error)
    percentile = q[0]
    qlist.append(percentile)
    


print(qlist)



# συγκρίνεται το lat κάθε εγγραφ΄ής με τα lat που έχουν οριστεί σαν όρια, 
# και η εγγραφή παίρνει την ΄ένδειξη της ζώνης στην οποία ανήκει
# η διαδικασία έχει αυτοματοποιηθεί ώστε να λειτουργεί για κάθε επιθυμητό αριθμό partitions
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

# μετατροπή του θ σε μοίρες
diff1 = theta / 110.567

# πρόσθεση στήλης με όρια τα οποία αν το "lat+-θ" ξεπερ΄άσει, να πρέπει να αντιγραφεί αυτή η εγγραφή για νέα ΖΩΝΗ.
df11 = df10.withColumn("sum_plus", col("lat")+diff1)
df11 = df11.withColumn("sum_minus", col("lat")-diff1)

# προσωρινή δημιουργία ενός sql πίνακα βάσει του dataframe μας
df11.createOrReplaceTempView("table10_0")

# spark sql ερωτήματα ώστε να βρεθούν ποιες εγγραφές πρ΄έπει να αντιγραφούν
# η διαδικασία είναι πλήρως αυτοματοποιημένη ώστε να λειτουργεί για οποιοδήποτε αριθμό partitions 
# και άρα για οποιαδήποτε ποσοστημόρια υπάρξουν.
# δημιουργείται python dictionary, με key κάθε ζώνη που περιμένει νέες εγγραφές, 
# και values τις έξτρα εγγραφές που θα "υποδεχτεί" κάθε ζώνη 
extra_dfs = {}    
for i in range(partitions_n):
    if i == 0:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id > 0 AND sum_minus < {} ) ".format(qlist[0]))
    
    if i >0 and i<partitions_n-1:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id < {} AND sum_plus > {} ) OR ( set0 = 'B' AND zone_id > {} AND sum_minus < {} ) ".format(i, qlist[i-1], i, qlist[i]))
        
    if i == partitions_n -1:
        extra_dfs["exs{0}".format(i)] = spark.sql("SELECT * FROM table10_0 WHERE ( set0 = 'B' AND zone_id < {} AND sum_plus > {} ) ".format(i, qlist[i-1]))
        

# διαγραφή των στηλών που δεν μας ενδιαφέρουν πλέον
df11 = df11.drop('sum_plus', 'sum_minus')

# για κάθε σύνολο 'εξτρα εγγραφών', διαγράφονται οι στήλες που δεν χρειαζόμαστε πλέον
# και προστίθεται μια στ΄ηλη με ένδειξη την επιπλέον ζώνη που πλέον θα "ανήκουν"
j=0
extra_dfs_final_dict = {}
for key, value in extra_dfs.items():
    extra_dfs_final_dict["exs{0}".format(j)] = extra_dfs[key].drop('sum_plus', 'sum_minus').withColumn("zone_id",lit(j))
    j+=1
    
# ένωση όλων των εγγραφών, αρχικών και έξτρα.
dfs = []
dfs.append(df11)
for key, value in extra_dfs_final_dict.items():
    dfs.append(extra_dfs_final_dict[key])
    
dff = reduce(DataFrame.unionAll, dfs)    

dff.cache()



# λογω της ιδιομορφίας του τοπικού ψευδο-clustering, παρατηρήθηκε ότι προκειμένου να μοιραστούν τα δεδομένα σε n partitions,
# πρέπει να δοθεί σαν είσοδος στην εντολή repartition, προσεγγιστικά η τιμή n*6
no_of_partitions = partitions_n*6

# διαχωρισμός των δεδομένων σε partition με βάση τη στήλη της ζώνης
dffj1 = dff.repartition(no_of_partitions, col("zone_id"))

# δημιουργία sql πίνακα
dffj1.createOrReplaceTempView("dffj")

# δημιουργία δύο dataframes, ένα για κάθε κατηγορία των δεδομένων. (δηλαδή από ποιο σύνολο είχαν προέρθει)
df_A = spark.sql("SELECT * FROM dffj where set0 = 'A'")
df_B = spark.sql("SELECT * FROM dffj where set0 = 'B'")



df_A.cache()
df_B.cache()



#print("Number of partitions: {}".format(dffj1.rdd.getNumPartitions())) 
# ποσότητες διαμοιρασμού δεδομένων, δηλαδή πόσες εγγραφές αντιστοιχούν σε κάθε partition
print('Partitioning distribution: '+ str(dffj1.rdd.glom().map(len).collect()))



# δημιουργία συνάρτησης για cross join μεταξύ δύο πινάκων, με όρισμα ένα query που υπολογίζει αν δύο σημεία βρ΄ίσκονται
# σε απόσταση μικρότερης του θ που έχουμε ορίσει
def distance(df1, df2):
    df1.createOrReplaceTempView("table1")
    df2.createOrReplaceTempView("table2")
    dfjoin = spark.sql("SELECT table1.name, table2.name from table1 cross join table2 where (acos(sin(radians(table1.lat)) * sin(radians(table2.lat)) + cos(radians(table1.lat)) * cos(radians(table2.lat)) * cos(radians(table1.lon - table2.lon))) * 6372.8)< {} ".format(theta))
    #return dfjoin.show()

start_time = time.time()

# εκτέλεση της συνάρτησης για τη σύζευξη
distance(df_A, df_B)

end_time = time.time()
print("{} seconds".format(end_time - start_time))

