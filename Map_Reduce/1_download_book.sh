#!/bin/bash

#Esta primera linea se utiliza para leer el archivo que se manda por la terminal
f=$1
#Esta variable la usare como un contador, para numerar cada txt nuevo que cree para guardar los libros
cont=1
#------------------------------ LOOP -----------------------------
#En mi looop se utiliza un while para ir leyendo linea por linea del archivo
while IFS= read -r line
do
    #Esta linea de comando la utilizamos para descargar los archivos y guardarlos en un txt enumerando 
     wget "$line" -O input/libros$cont.txt
     #En este comando voy aumentando el contador 
     cont=$((cont+1))
# Aca finaliza el loop y el archivo que yo escriba lo va a leer
done < $f
#Comando para utilizar el codigo en la terminal
# En caso de que solucite permiso use primero el comado
#chmod +x 1_dowload_book.sh
#./1_download_book.sh libros.txt