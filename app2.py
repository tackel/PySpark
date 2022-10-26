import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app2").getOrCreate()

#Broadcast
BroadcastVar = spark.sparkContext.broadcast([1,2,3])
print("")
print(BroadcastVar.value)
print("")

# acumulador 
accum = spark.sparkContext.accumulator(0)
sumatorioError = 0

def myfunc(x):
    global sumatorioError
    accum.add(x)
    sumatorioError += x

rdd = spark.sparkContext.parallelize([1,2,3,4,5])

rdd.foreach(myfunc)
print("")
print(accum)
print(sumatorioError)
print("")



# ejecutar desde consola:
# ./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 app2.py 