from pyspark import SQLContext, SparkContext
from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType
import re

sc = SparkContext()
spark = SQLContext(sc)

# header='true' convierte la primera fila en el nombre de las colulmnas
# df_genomeScores = spark.read.csv('ml-25m/genome-scores.csv', header='true')
# df_genomeTags = spark.read.csv('ml-25m/genome-tags.csv', header='true')
# df_links = spark.read.csv('ml-25m/links.csv', header='true')
# df_movies = spark.read.csv('ml-25m/movies.csv', header='true')
df_ratings = spark.read.csv('ml-25m/ratings.csv', header='true')
# df_tags = spark.read.csv('ml-25m/tags.csv', header='true')

df_ratings.printSchema() # Nombre de las columnas:tipo de dato
#df_ratings.show()    

"""Cuál es el porcentaje de usuarios (%) que le han
dado a las películas una calificación promedio
superior a 3 estrellas?"""

## Obtener el promedio de calificación por usuario
# la función .cast(DataType()) retorna el dataframe cambiando el tipo de dato de una columna
df_ratings = df_ratings.withColumn("rating", col("rating").cast(DoubleType()))
df_ratings = df_ratings.withColumn("userId", col("userId").cast(IntegerType()))
promedio_ratings = df_ratings.groupBy("userId").agg(avg("rating").alias('average')).sort('userId')

# Obtener el porcentaje de usuarios con promedio mayor a 3
condicion = promedio_ratings.average > 3
filter_ratings = promedio_ratings.filter(condicion)

porcentaje = (filter_ratings.count()*100) / promedio_ratings.count()

print(f'Porcentaje de usuarios que puntuan por promedio mayor a 3 estrellas: {porcentaje:.2f}%')