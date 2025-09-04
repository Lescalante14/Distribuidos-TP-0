#!/bin/bash
echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"
python3 generar-compose.py $1 $2
# delete csv files in .data folder
rm -f .data/agency-*.csv
sleep 2