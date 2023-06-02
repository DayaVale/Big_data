# Primera fase del proyecto
## Luna Duran, Dayana Gonzales y Emanuel Naval
---
# Problemática

La identificación precisa de si un tweet se clasifica como ciberacoso y de qué tipo es una problemática compleja debido a la interpretación subjetiva de los mensajes en línea y a la variedad de formas que puede adoptar el ciberacoso. Esto incluye el ciberacoso por etnia, edad, género o religión. Es importante considerar el contexto, la intención y los patrones de comportamiento para determinar si un tweet en particular constituye acoso. La combinación de análisis automatizados y la intervención humana es necesaria, y la conciencia sobre el ciberacoso es fundamental para abordar esta problemática y promover un entorno en línea seguro.

El uso del Big Data es relevante para abordar esta problemática, reduciendo los tiempos de computo para la creación de un modelo de aprendizaje automático el cual sea entrenado con un gran volumen de datos que sea capaz de clasificar tweets como casos de ciberacoso o no, e incluso identificar el motivo del acoso.

# Elección del dataset

De acuerdo a la problemática planteada, el dataset a utilizar será [Cyberbullying Classification](https://www.kaggle.com/datasets/andrewmvd/cyberbullying-classification), el cual contiene más de $47000$ tweets etiquetados según la clase de ciberacoso:

* Edad
* Origen étnico
* Género
* Religión
* Otro tipo de ciberacoso
* No ciberacos

Los datos se han equilibrado para que contengan ~8000 de cada clase. De este modo, el dataset solo cuenta con dos columnas: *tweet_text* y *Cyberbullying_type*.

# Cómo se realiza la limpieza de los datos

Revisar el archivo de jupyter notebook (.ipynb) el cual contiene el paso a paso para el procesamiento y limpieza de datos.

# Definición de la metodología para abordar la problemática

- Procesos a seguir utilizando SparkML y cluster (AWS)
    - Por medio de pyspark y de manera local se definirá un modelo de clasificación para el problema dado, el cual será entrenado con el conjunto de datos de entrenamiento y se utilizará dentro de una estructura en la nube en AWS EMR. Dicha estructura consistirá en un nodo master con 4 nodos esclavos con tamaños de 4.large y 4.xlarge para el nodo master y los nodos esclavos respectivamente.
- Pasos específicos:
    - Transformaciones: Proceso a seguir para la transformación de datos debido a que se van a clasificar textos en lenguaje natural.
        - Limpieza y normalización de los datos (tweets) eliminando stopwors, links, #, @ y las palabras con longitud menor a 3 y caracteres especiales.
        - Creación de la bolsa de palabras.
        - Codificación de la bolsa de palabras.
        - Definición del Pipeline para la creación del modelo.
        - División del dataset en conjunto de entrenamiento y prueba. (70/30)
    - Construcción de un modelo entre las siguientes opciones:
        - Regresión logistica / Naive Bayes / Random Forest
    - Entrenamiento del modelo
        - Entrenamiento del modelo a partir del conjunto de datos de entrenamiento (70%)
    - Evaluación del desempeño
        - Obtener la predicción del conjunto de datos de prueba (30%)
        - Utilizar una métrica para la evaluación final del modelo
            - Visualización de una matriz de confución
            - Obtención de los valores de las métricas pertinentes (Recall, Precision, Accuracy, F1-score).
            - Creación de una tabla comparativa para cada una de las métricas obtenidas.
    - Conclusiones de los resultados obtenidos a partir de la información recolectada.