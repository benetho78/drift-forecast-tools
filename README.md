# Scripts descarga datos pronosticos

## Requerimientos

 - Comando NCO
 - python > 3.7
 - xarray, dask, siphon
 
## Herramientas:

 - `bin/download-fnmoc-amseas-forecast-ncks.py` : Descarga un subconjunto del pronostico FNMOC de la region AMSEAS.
 - `bin/download-gfs-forecast.py` : Descarga un subconjunto del pronostico GLOBAL GFS 1/4 de grado.

## Sobre los datos:

**Fleet Numerical Meteorology and Oceanography Center AMSEAS Forecast**

Pronostico de corrientes con modelo HYCOM para la region de los mares americanos
(AMerican SEAS) que se compone por el Golfo de México y parte central este del Atlantico.
Este pronostico se genera diariamente y contiene 4 dias de pronostico con un paso de tiempo
de 3 horas, con resolución horizontal de 3km y 40 niveles en vertical.

Variables disponibles:
 temperatura agua,salinidad, elevacion, corrientes en componentes u y v.


Servidores descarga de datos:

Servidor thredds ncei:
https://www.ncei.noaa.gov/thredds-coastal/catalog/amseas/catalog.html
Carpeta con pronostico: amseas_20201218_to_current/ 


**Global Forecast System**

Pronostico global climatico, para una docena de atmosferas, con variables como temperatura, precipitacion
vientos, concentración de ozono, entre otras.  El sistema acopla cuatro modelos para mostrar de forma mas fiel las 
condiciones climaticas.

Servidor thredds UCAR
https://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p25deg/catalog.html?dataset=grib/NCEP/GFS/Global_0p25deg/Best

Series procesadas, con el ultimo pronostico.




