# covid - OLAP

Base de datos OLAP con cantidad de casos, muertes, infecciones y vacunas de covid19 desde 2019 hasta 2024.

## Instrucciones
Crear base de datos covid, correr el covid.sql para crear las tablas

Hacer el setup del environment de python
python -m venv venv

Windows
venv/scripts/activate
Unix
source venv/bin/activate

pip install

Luego si estas en pycharm tenes que ir a Python Interpreter > Add Interpreter > Local Interpreter > VirtualEnv > Existing
Se elige el que se te creo en la carpeta y listo

Link a CSV
https://ourworldindata.org/grapher/gdp-per-capita-maddison
https://www.who.int/data/data-collection-tools/who-mortality-database
https://ourworldindata.org/grapher/total-healthcare-expenditure-gdp
https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv
