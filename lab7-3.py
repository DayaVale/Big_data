from pyspark import SQLContext, SparkContext
from pyspark.sql.functions import avg, col, substring_index, count
from pyspark.sql.types import DoubleType, IntegerType


sc = SparkContext()
spark = SQLContext(sc)

df_movies = spark.read.csv('ml-25m/movies.csv', header='true')
df_ratings = spark.read.csv('ml-25m/ratings.csv', header='true')

# Cambio en la columna género
df_movies = df_movies.withColumn("genres", substring_index("genres", "|", 1))

df_movies.printSchema()
df_ratings.printSchema()

# Unir las dos tablas
df_joined = df_movies.join(df_ratings, df_movies.movieId == df_ratings.movieId, "inner")

# Valoración y agurpar de los generos
promedio_genres = df_joined.groupBy("genres").agg(avg("rating").alias('average')).sort('average', ascending=False)

promedio_genres.show(n=5)

