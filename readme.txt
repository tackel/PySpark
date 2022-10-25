INSTALAR SPARK

# Actulizar el sistema
sudo apt-get update

tener instalado python

instalar java
sudo apt install default-jre
confirmar con java --version

para ir a la raiz
sudo su 

Creo carpeta
mkdir -p /opt/spark
cd /opt/spark

dentro de la carpeta ejecuto el archivo de descarga de la doc de apache spark
wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz

ls para listar los archivos de la carpeta

extraer los archivos del fichero con:
tar xvf spark-3.3.0-bin-hadoop3.tgz

en una neva terminal:
cd ..
gedit .\bashrc
modificar este archvio al final con estos codigos y guardar:
export SPARK_HOME=/opt/spark/spark-3.3.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin

en otro consola pones spark-shell y ya funciona o pyspark y tambien se ejecuta para python
LISTO!!!

--------------------------------------------------

entro a la carpeta con los dataset
poner un ls -lh para listar los archivos y ver datos en megas y gigas

ARRRANCAR UN CLASTER DE SPARK EN STANDAR OLNE
en este modo tenemos el master y el slave o worker en la misma maquina para arrancar con
 el tema
vas a la carpeta de spark y a bin
en mi caso: cd /opt/spark/spark-3.3.0-bin-hadoop/sbin

luego: ./start-master.sh
arrancar un slave: ./start-slave.sh spark://127.0.0.1:7077
cd .. para ir a carpeta anterior
cd bin # entrar ahi
ls para listar

abrir una shell con pyspark: ./pyspark
YA PUEDO UTILIZAR TODO, Y ARMAR UN DATAFRAME

df = spark.read.options(header='True', inferSchema='True').csv('/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv')

ya se puede interactuar ejemplo:
df.count()
df.printSchema()

df.select("event_type").distinct().show() # asi ves todos los eventos disponibles tambien podes hacerlo con marcas "brand"
df.select(["product_id"]).filter("event_type='cart'").show() # todos los product_id con un event_type

df.select(["product_id"]).filter("event_type='cart'").first() # buscamos el primer producto

buscamos todas las seciones donde aparece un event_type y un producto en particular:
df.select(["user_session"]).filter("event_type='cart'").first()

sesions = df.select(["user_session"]).filter("event_type='cart' AND product_id=5844305")
prducts=df.select(["product_id"]).filter("event_type='cart' AND product_id<>5844305").filter(df["user_session"].isin(sesions["user_session"]))


prducts.select("product_id").show()

prducts.select("product_id").count()
product=prducts.select("product_id").distinct()
product.select("product_id").count()

con esto ya se cuales son los productos relacionados o que se vendieron con este producto en particular
ahroa exporto los datos 

product.write.mode("overwrite").csv('/home/tackel/Documentos/Programacion/PySpark/result1')

esto te crea la carpeta resulto1 y dentro hay muchos ficheros .csv por que particiona la data para hacer muchas cosas a la ves. Tambien hay
un fichero SUCCES al final para que sepas que termino el proceso

para verlo: desde otra terminal: cat part-00000-526be52f-aab8-48bb-89b7-7df09cbd31ce-c000.csv # nombre dle archivo

para lso dataframe tambien permite trabajar con lenguaje sql

df.createOrReplaceTempView("data")
spark.sql("select * from data limit 3").show()
spark.sql("select * from data where event_type='cart' limit 3").show()


----------- - RDD -----------------
dataset distribuidos

arrancar un claster: en este modo tenemos el master y el slave o worker en la misma maquina para arrancar con
 el tema
vas a la carpeta de spark y a bin
en mi caso: cd /opt/spark/spark-3.3.0-bin-hadoop/sbin

luego: ./start-master.sh
#### en el localhost:8080 se puede acceder a un dashboar donde nos da los datos del master 
iniciamos un worker que es igual que un slave que hicimos anterirmente:
y le pasamos lso datos de dodne esta el worker que aparece en el dashboar donde entramos en el localhost anterior

./start-worker.sh spark://tackel-300E4A-300E5A-300E7A:7077

asi el dashboard nos da los datos del worker iniciado
cd .. para ir a carpeta anterior
cd bin # entrar ahi
ls para listar

abrir una shell con pyspark: ./pyspark
YA PUEDO UTILIZAR TODO, Y ARMAR UN DATAFRAME

df = spark.read.options(header='True', inferSchema='True').csv('/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv')
df.count()
df.printSchema()

#### OBTENER CUANTOS PRODUCTOS SE VENDIERON DE UNA MARCA EN particular CON RDD

#encontrar las marcas disponibles:
df.select("brand")distinct().show()
#obtener un RDD,  el df.rdd es para paralelizar los datos del df, pasarlo a rdd.

def myfunc(s):
    if s["brand"]=="riche" and s["event_type"]=="cart":
        return [ ( s["product_id"], 1) ]
    return []

# flatMap o map la diferencia es que map a cada registro le aplica la func y devuelve otro registro, el flap map devuelve entre 0 y n registros.
lines = df.rdd.flatMap( myfunc ).reduceByKey( lambda a, b : a+b ) 
# el reduceByKey toma cada key que devuelve el flatMap y le aplica una lambda

for e in lines.collect():
    print(e)

#tambien puedo hacer esto:
print( lines.take(20)
# da los 20 primeros
# teenr cuidado con el collect y el take por que si son mucho registros va a tardar una eternidad, 

lines.toDF().show()

# guardarlos

lines.saveAsTextFile('/home/tackel/Documentos/Programacion/PySpark/resultConRDD')
# entramos a esta carpeta con otra terminal y podemos ver cada uno poniendo:
cat nombreDelArchivo

# los metodos mas usados son: map, flapMap, filter, reduceByKey

# ############PARA NO USAR LA CONSOLA, PODEMOS CREAR UNA APLICACION QUE HAGA EL proceso
#cerramoe primero:
exit()

#ponemos en carpeta bin y creamos una aplicacion
# tener en cuenta quecon la consola estan hechos los import y aca hay que inicializar nosotros junto con los import

nano app1.py # creo que crea un arhivo py

# en la app hacemos exactamente lo mismo que por consola
#solo cambiamos una cosa para que quede mejor

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

./spark-submit --master acaElMasterDelDarhboard NombreAplicacion dataSetEntrada dataSetSalida
# en mi ejemplo seria asi:
./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 app1.py "/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv" "/home/tackel/Documentos/Programacion/PySpark/resultConRDD"

# puede dar error por falta de memori, se le puede poner, si es que tengo, asi:
# o por que ya existe el archivo de salida, eliminarlo
rm -R ../../resultConRDD/ # esto funciona para su ejemplo
./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 --config spark.executor.memory=4g --config spark.driver.memory=4g "/home/tackel/Documentos/Programacion/PySpark/dataSet/*.csv" "/home/tackel/Documentos/Programacion/PySpark/resultConRDD"


#si todo va bien, entras al dashboar y deberia estar ahi el proceso. Y ya deberia estar generado el archvo en la carpeta de salida seleccionada

                          # ####### ACUMULADORES ##################3

# generamos un archivo .py
nano app2.py

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app1").getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv(sys.argv[1])

#Broadcast
BroadcastVar = spark.sparkContext.Broadcast([1,2,3])
print("")
print(BroadcastVar.value)
print("")

# acumulador 
accum = spark.sparkContext.accumulator(0)
sumatorioError = 0

def myfunc(x):
    global sumatorioError
    sumatorioError += x

rdd = spark.sparkContext.parallelize([1,2,3,4,5])

rdd.foreach(myfunc)
print("")
print(accum)
print(sumatorioError)
print("")

# ejecutar desde consola:
./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 app2.py 