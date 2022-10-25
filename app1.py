import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv(sys.argv[1])

def myfunc(s):
    if s["brand"]=="riche" and s["event_type"]=="cart":
        return [ ( s["product_id"], 1) ]
    return []

lines = df.rdd.flatMap( myfunc ).reduceByKey( lambda a, b : a+b ) 

lines.saveAsTextFile(sys.argv[2])



## luegoo en consola ejecutamos este arhivos:
# ./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 app1.py "/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv" "/home/tackel/Documentos/Programacion/PySpark/resultConRDD"
# puede dar error por falta de memori, se le puede poner, si es que tengo, asi:
# o por que ya existe el archivo de salida, eliminarlo

# ./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 --config spark.executor.memory=4g --config spark.driver.memory=4g "/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv" "/home/tackel/Documentos/Programacion/PySpark/resultConRDD"
