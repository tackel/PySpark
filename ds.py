from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Inicializamos contexto
sc = SparkContext("local[2]", "NetworkWordCount") # se pone nombre de aplicacon y el master
ssc = StreamingContext(sc, 10) # se pone contexto de spark y cada cuanto segundo se procesan datos

lines = ssc.socketTextStream("localhost", 9090) # podria ser un fileTextStream u otros tipos de origen de datos
# el no va a estar escuchando en este puerso sino que va conectarse, por lo que hay que poner algo a funcionar en este puerto 9090

# Proceso de datos
words = lines.flatMap(lambda line: line.split(" ")) # tranformar cada palabra en un registro
pairs = words.map(lambda word: (word, 1)) # hace un mapeo para ponerle a cada palabra el valor de 1
wordCounts = pairs.reduceByKey(lambda x, y: x + y) # por cada palabra que encuentra suma

# se hace con mapreduce para paralelizar el trabajo
wordCounts.pprint()# imprime los 10 primero registros


ssc.start()
ssc.awaitTermination()
