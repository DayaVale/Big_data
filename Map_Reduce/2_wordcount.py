#Punto 4 Individual
#Objetivo: Cada estudiante debe identificar las 20 palabras comunes y mas recurrentes en los libros que tengan una longitud mayor a 5 caracteres

#Librerias
from mrjob.job import MRJob
from mrjob.step import MRStep
import re


WORD_REGEX = re.compile(r"\b\w+\b")

class Lab1(MRJob):

    #Realizaremos el ejercicio utilizando tres pasos, donde el primero conta del mapper, combiner y un reducer.
    def steps(self):
        return[
            MRStep(mapper = self.mapper_five_len,
                   combiner = self.combiner_sum_word,
                   reducer = self.reducer_sum_total),
            MRStep(reducer = self.reducer_sort_word),
            MRStep(reducer = self.reducer_top_20)
        ]

    #Step1: Mapper: En el mapper recibimo linea por linea y tomamos unicamente las palabras con longitud mayor a 5.
    def mapper_five_len(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            if len(word) > 5:
                yield(word.lower(), 1)

    #Step1: Combiner: Decidi realizar un combiner, ya que ingresaremos 15 libros con una gran cantidad de palabras.
    #Entonces lo usaree para disminuir la carga del reducer, pero realizara lo mismo que el reducer y es sumar las repeticiones de 
    #las palabras.
    def combiner_sum_word(self,word,values):
        total = sum(values)
        yield(word, total)

    #Step1: Reducer: Este reducer lo unico que hace es sumar los valores para saber la cantidad de palabras.
    def reducer_sum_total(self,word,value):
        total = sum(value)
        yield None, (total,word)
           
    #Step2: Este paso consiste en recibir todas las tupls del total en la cantidad de palabras y la palabra, lo va a organizar
    #Por el primero de la tupla, es decri, por la cantidad de palabras.
    def reducer_sort_word(self,_,value):
        x = sorted(value,reverse = True)
        yield None, x

    #Step3: Para este ultimo paso, resibira una lista de tuplas organizadas, por la cantidas y así es más facil tomar las 20 palabras más
    #populares.
    def reducer_top_20(self,_,value):
        lista1 = list(value)[0]
        for i in range(20):
            repeticion, palabra = lista1[i]
            yield palabra, repeticion
    #Obteniendo así la palabre y cuando se repite. Solo saldran las 20 más populares.



if __name__ == "__main__":
    Lab1.run()

# Para correr el programa siga el siguiente comando:
#python 2_wordcount.py input/* > 3_out.txt