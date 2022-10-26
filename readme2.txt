# Arrancar un master y un worker como hicimos ateriormente
# abrir el dashboard
# desde la carpeta sbin
./start-master.sh
./star-worker.sh
# crear aplicacion
nano ds.py

# en el archivo ds.py sigue

# para que funcione el soccet
# desde carpeta /bin en una terminal nueva

nc -lk 9090
#y abajo:
hola mundo
# esto se lo manda al que se conecte 
 

# ejecutamos la app1
./spark-submit --master spark://tackel-300E4A-300E5A-300E7A:7077 /home/tackel/Documentos/Programacion/PySpark/ds.py 