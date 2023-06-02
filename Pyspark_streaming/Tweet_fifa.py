#Librerías
import socket
import time

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode ,split ,window
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum,concat_ws, collect_list,col,desc

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("FifatweetStreaming")\
        .getOrCreate() \
   
    spark.sparkContext.setLogLevel("ERROR")
   
   
    # Leemos los datos que van llegando mediante socket.
    fifaDF = spark.readStream.format("socket").option("host", '0.tcp.ngrok.io')\
             .option("port", 12424).load()

   
    # Realizamos la division de los datos mediante una coma para tomas 
    # los valores que necesitamos

    ExpRegul  = ','
   
    linea =fifaDF.withColumn('divir_values',  split(fifaDF.value, ExpRegul))    

    '''Dividimos cada una de las  columnas y ademas realizamos dos filtros donde el 
     primero nos va  tomar los datos que tienen como id números y el otros donde los
      sentimientos sean Positivos, negativas y neutrales, tales que tengamos solo los
       datos que llegaron de la mejor manera. '''
    
    fifaDF = linea.withColumn('id', linea['divir_values'].getItem(0)) \
           .withColumn('date_time', linea['divir_values'].getItem(1)) \
           .withColumn('Number_likes',linea['divir_values'].getItem(2)) \
           .withColumn('source', linea['divir_values'].getItem(3)) \
           .withColumn('tweet', linea['divir_values'].getItem(4)) \
           .withColumn('sentiment', linea['divir_values'].getItem(5))\
           .filter(col('id').rlike("^[0-9]+$"))\
           .filter(col('sentiment').rlike("(positive)|(negative)|(neutral)"))
           
    ''' Para resolver el problema inicial agruparemos por el sentimiento y sumaremos el número de likes,
    así en cada batch podremos ver cual es el sentimiento que más se genero en la Copa fifa 2022 a  partir
    de la suma de los likes.'''
     
    sum_sentimientos = fifaDF.groupBy(fifaDF.sentiment).agg(sum('Number_likes').alias('num_like'))\
                       
    ''' Genere la siguiente función para poder imprimir cada uno de los batch que se generen
    en el lapso de tiempo de 24 minutos '''

    def write_batch(batch_df, batch_id):
        file_name = "out.txt"
        '''Con la funcion collect podemos tomar los valores de la tabla dada por cada una de las filas'''
        result = [str(row) for row in batch_df.collect()]
        ''' Pongo la opción "a" para que escriba en el mismo archivo pero no rescriba en 
        lo que ya se tiene.'''
        with open(file_name,"a") as f:
            f.write(f"-----------------------batch_{batch_id}---------------\n")
            f.write("\n".join(result))
            f.write("\n")

    '''Decidi no poner por el momenta la direccion del checkpoint para que sea tolerante a fallos
    para no tener problemas al moento de subirlo a aws pero se coloca de la siguiente manera
    checkpointDir = 'temp/out'
    .option("checkpointLocation", checkpointDir)\ '''
   
    ''' La query de salida tiene el outmode con update ya que yo quiero que cada vez que se genere 
    una nueva tabla me imprima solamente las columnas que se estan actualizando y asi poder visualizar el sentimiento que
    sigue aumentando su suma de likes. Además puse un trigger de 5 segundos para que el volumen de los datos no tengan 
    problema y así no se ejecute la query instaneamente.'''

    query = sum_sentimientos.writeStream\
                        .foreachBatch(write_batch)\
                       .outputMode("update")\
                       .trigger(processingTime="5 seconds")\
                       .start()
   
    ''' Este time.sleep le va a decir que cuando se cumplan los 24 minutos es decir 1440 segundos,
    pare la query y deje de funcionar.'''
    time.sleep(1400)
    # stop the query
    query.stop()
    '''Este codigo se corrio en aws y se obtuvieron los out.txt de alli a partir del siguiente comando:
    pscp -i ruta/a/llave.ppk usuario@direccionIPNodoMaestro:ruta/del/archivo.txt ruta/local/donde/guardar/
    se utilizo putty para la conexion con ssh, ya que se realizo mediante windows.'''