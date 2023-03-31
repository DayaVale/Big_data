#Librerias
from pyspark import SQLContext, SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType
import re

#Objetivo: Para cada género, encuentre la calificación promedio del género y la cantidad de películas
#que pertenecen a este género. Si una película pertenece a más de un género, considere el
#mismo puntaje en cada género.


#Inicializo el contexto de spark y el contexto SQL para utilizar las funciones SQL 
# En una api de alto Nivel
sc = SparkContext()
spark = SQLContext(sc)

#Cargo los datos y los agrego a un DataFrame. Para esto necesitamos la base de datos Movies.csv y ratings.csv
df_movies = spark.read.csv('ml-25m/movies.csv', header='true')
df_ratings = spark.read.csv('ml-25m/ratings.csv', header='true')

#Vemos el esquema de cada una de los dataframe, para ver los datos que tenemos y si tenemos que cambiar algun tipo de dato
#df_movies.printSchema()
#df_ratings.printSchema()

#Cambiaremos el tipo de dato para ratings en el dataframe df_ratings, ya que es string y lo necesitamos float.
# Con WithColum añadimos a la columna ratings donde con cast cambiamos el tipo de dato.
df_ratings = df_ratings.withColumn("rating", F.col("rating").cast(DoubleType()))
#df_ratings.printSchema()
df_ratings.show()
#df_movies.show()


New_movies = df_movies.withColumn("genres",F.split(F.col("genres"),"\\|"))
New_movies = New_movies.withColumn("genres", F.explode("genres") )
New_movies = New_movies.withColumnRenamed("movieId","IdMovie")
New_movies.show()

# Realizare un Join para unir el nuevo dataframe de peliculas con el dataframe de Ratings y así lograr el objetivo
df_join = New_movies.join(df_ratings, New_movies.IdMovie == df_ratings.movieId, "outer")
df_join.show()

#Necesito que por cada genero me cuente la cantidad de peliculas y el promedio de calificación.
result = df_join.groupBy("genres").agg(F.avg("rating").alias("average"),F.countDistinct("IdMovie").alias('Num_movie'))
result = result.persist()
#result = result.withColumn("average", F.col("average").cast(StringType()))
#result = result.withColumn('Num_movie', F.col('Num_movie').cast(StringType()))
result = result.withColumn("genres,average,num_movie", F.concat_ws(',',F.col("genres"),F.col("average"),F.col("Num_movie"))).repartition(1)

result.select("genres,average,num_movie").write.text("3_out.txt")
#result.show(truncate=False)