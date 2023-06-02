#Integrantes 
#---------------------------------------------------
# * Dayana Valentina Gonzalez Vargas
# * Luna Gabriela Duran Perez
# * Emanuel Naval Oviedo
#-----------------------------------------------------
# Importación de librerías
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder.appName("cyberbullying_classification")\
            .getOrCreate()
    
    # Url para acceder a los datos sea en local como en la nube
    url = "s3://mybuckesito/cyberbullying_tweets.csv"
    # Carga de los datos con opciones que permiten separar por comas
    data = spark.read.option("quote", "\"") \
                     .option("escape", "\"") \
                     .option("multiline", True) \
                     .csv(url, sep = ",", header='true')

    ## Limpieza y manejo de los datos
    ## Inicialización de las funciones para el pipeline
    # Tokenizador con expresion regular
    regexTokenizer = RegexTokenizer(inputCol="tweet_text", outputCol="words", pattern="\\W")
    # Eliminador de stopwords
    stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered")
    # Contador para la bolsa de palabras
    countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)
    # Codificación de la variable respuesta
    label_stringIdx = StringIndexer(inputCol = "cyberbullying_type", outputCol = "label")

    # Pipeline inicial para tokenizar y remover stopwords
    pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover])
    pipelineFit = pipeline.fit(data)
    dataset = pipelineFit.transform(data)

    # Eliminación de las palabras de longitud < 3 para las palabras filtradas
    expr_sql = "filter(filtered, x -> length(x) > 3)"
    dataset = dataset.withColumn("filtered", expr(expr_sql))

    # Pipeline final para la creación de la bolsa de palabras y codif. de la var. respuesta
    pipeline = Pipeline(stages=[countVectors, label_stringIdx])
    pipelineFit = pipeline.fit(dataset)
    dataset = pipelineFit.transform(dataset)

    # División del dataset en conjunto de entrenamiento y prueba
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 23)

    # Se realizara la clasificación utilizando regresión logistica con cross validation para poder
    # obtener los mejores resultados para llegar un modelo más preciso en su clasificación y con menor perdida

    # Modelo de regresión logistica con un maximo de iteraciones, parametro de regularización y elasticnet que es 
    # la regularización de rigde.
    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
    
    # Se crea paramGrid para probar diferentes valores de los hiperparametros que se mencionaron anteriormente y 
    # Encontrar los parametros que generen el mejor modelo. 
    GridCV = (ParamGridBuilder()
                .addGrid(lr.regParam, [0.1,0.3, 0.5]) # parametro de regularización
                .addGrid(lr.elasticNetParam, [0.0, 0.2, 1]) # tipo de regularización (ridge, lasso o ambas)
                .build())
    
    # Inicializamos el evaluador.
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    
    # Creamos 5 folders de cross validación para encontra el mejor modelo como estimador tomamos la regresión 
    # logistica.
    cv = CrossValidator(estimator=lr, \
                        estimatorParamMaps=GridCV, \
                        evaluator=evaluator, \
                        numFolds=5)
    
    # Entrenamos el modelo
    cvModel = cv.fit(trainingData)

    # Evaluamos el modelo y obtenemos las predicciones
    predictions = cvModel.transform(testData)
   
    # Generamos un dataframe que seleccione el tweet, el tipo de ciber acoso, la etiqueta que se le asigno al 
    # tipo de acoso y la predicción resultante del mejor modelo.
    Result = predictions.select("tweet_text","cyberbullying_type","label","prediction")\
            .orderBy("cyberbullying_type", ascending=True) 

    # Evaluamos las metricas de este modelo con el Accuracy, f1 y hammingloss para ver como es la presición del modelo.
    # Accuracy : presición del modelo
    # f1 : número de clasificadas correctas positivas / el total de clasificadas como positivas
    # Hamming Loss: número de incorrectas/ el total.

    Accuracy = evaluator.evaluate(predictions,{evaluator.metricName: "accuracy"})
    
    evaluator.setMetricName("f1")
    f1 = evaluator.evaluate(predictions)

    evaluator.setMetricName("hammingLoss")
    HLoss = evaluator.evaluate(predictions)

    # Generamos un nuevo datafame con las metricas proporcionadas.
    # Define el esquema del DataFrame
    schema = StructType([
        StructField("Metrica", StringType(), nullable=True),
        StructField("Valor", FloatType(), nullable=True)
    ])

    # Crea un DataFrame a partir de una lista de filas
    data = [("accuracy", Accuracy), ("f1-score", f1), ("HammingLoss", HLoss)]
    df = spark.createDataFrame(data, schema)

    # Conevertimos el dataframe de spark en uno de pandas
    dfpandas1 = df.toPandas()
    Resultpandas1 = Result.toPandas()

    # Pasamos el dataframe de pandas a html
    htmldf = dfpandas1.to_html()
    htmlresult = Resultpandas1.to_html()

    # Escribimos los resultados en un html
    with open('resultados.html', 'w') as f:
        f.write(htmlresult)
        f.write(htmldf)

    # Con la siguiente linea de codigo migramos el resultado a un bucket de s3 para que la gente lo pueda ver.
    os.system("aws s3 cp resultados.html s3://mybuckesito/out/")   