from pyspark import SQLContext, SparkContext
from pyspark.sql.functions import avg, col, substring_index, count
from pyspark.sql.types import DoubleType, IntegerType
import re

sc = SparkContext()
spark = SQLContext(sc)

df_movies = spark.read.csv('ml-25m/movies.csv', header='true')
df_movies.printSchema()

# Cambiamos el tipo de dato de la col movieId a int
df_movies = df_movies.withColumn("movieId", col("movieId").cast(IntegerType()))

# Tomamos sólo el primer género de cada pelicula
df_movies = df_movies.withColumn("genres", substring_index("genres", "|", 1))

# Contamos por género
count_genres = df_movies.groupBy("genres").agg(count("movieId").alias('count')).sort('count')

count_genres.show()