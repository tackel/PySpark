from pyspark.sql import SparkSession

# creo la session
spark = SparkSession.builder.getOrCreate()

# importo los datos
df = spark.read.options(header='True', inferSchema='True').csv("/opt/*.csv")

# utilizo df, recordar que solo se ejecutan en un solo nodo, worker
df.count()
df.printSchema()
df.select("categories").distinct().show()

def myFunc(s):
  return [ ( s["categories"], 1) ]
  
# aca saco las distittas categorias pero con RDDs, que si se paralelizan
lines=df.rdd.flatMap(myFunc).reduceByKey(lambda a, b: a + b)