#!/usr/bin/env python

import os
import sys
import yaml
import argparse
import numpy as np
import xarray as xr
import datetime as dt
from dask.diagnostics import ProgressBar
from siphon.catalog import TDSCatalog

def getNearestIdxSlice(min, max, arr):
    return slice((np.abs(arr - min)).argmin(), (np.abs(arr - max)).argmin())


def getSafeOutputFilename(proposedFilename, fextension='nc', count=0):
    if os.path.exists(proposedFilename + '.' + fextension):
        if proposedFilename.split('_')[-1].isnumeric():
            count = int(proposedFilename.split('_')[-1])
            proposedFilename = '_'.join(proposedFilename.split('_')[0:-1])
        nproposedFilename = proposedFilename + '_' + str(count+1)
        return getSafeOutputFilename(nproposedFilename, fextension, count+1)
    else:
        return proposedFilename + '.' + fextension


def getDapUrls(forecastDate):
    '''
     Devuelve el listado de urls ordenadas para consultar los datos del pronostico.
    '''
    amseasThreddsCatalogURL = \
    'https://www.ncei.noaa.gov/thredds-coastal/catalog/amseas/amseas_20201218_to_current/{}/catalog.xml'\
    .format(forecastDate)

    cat = TDSCatalog(amseasThreddsCatalogURL)
    dapURLs = [ dapurl.access_urls['OPENDAP'] for dapurl in cat.datasets.values() ] 
    dapURLs.reverse()
    return dapURLs

def dap2DataArray(dapURLs, chunks={'time' : 1}):
    '''
     A partir de urls opendap conectadas por una dimension (tiempo), crea un dataArray
     con soporte para uso de dask.  
     Devuelve el DataArray si la consulta remota de datos es exitosa.
    '''
    try:
        dst = xr.open_mfdataset(dapURLs, concat_dim='time', combine='nested',
                            chunks=chunks, decode_times=False, parallel=True)
    except Exception as e:
        print('Failed to create mfDataset from opendap urls')
        print (str(e))
        sys.exit(1)

    return dst

def makeCFCompliant(dst):
    '''
     Agrega los atributos faltantes a los conjuntos de datos del pronostic fnmoc-amseas.
     Devuelve el DataArray con las modificaciones.
    '''
    # CF Compliant
    # Agregar los atributos necesarios al xarray.dataset que esta en memoria
    # para hacerlo "CF compliant"

    # Cordenadas 
    dst.coords['lon'] = dst.coords['lon'] - (180*2)   # Los datos de coordenadas para lon deben de ir de -180 a 0
                                                      # en unidades degrees_east
    dst.coords['lon'].attrs['axis'] = 'X'
    dst.coords['lon'].attrs['units'] = 'degrees_east'
    dst.coords['lon'].attrs['standard_name'] = 'longitude'

    dst.coords['lat'].attrs['axis'] = 'Y'
    dst.coords['lat'].attrs['standard_name'] = 'latitude'

    dst.coords['depth'].attrs['axis'] = 'Z'
    dst.coords['depth'].attrs['standard_name'] = 'depth'

    dst.coords['time'].attrs['axis'] = 'T'
    dst.coords['time'].attrs['standard_name'] = 'time'

    # Variables standard_names
    dst.variables['water_u'].attrs['standard_name'] = 'x_sea_water_velocity'
    dst.variables['water_v'].attrs['standard_name'] = 'y_sea_water_velocity'

    return dst




if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Parallel download for the FNMOC Amseas forecast', prog='download-fnmoc-amseas-forecast')
    parser.add_argument('--subset', '-s', action='store', dest='subsetconfig', help='yaml file with the subset to download.')

    args = parser.parse_args()
    # TODO Add verbose mode
    if args.subsetconfig:
        with open(args.subsetconfig, 'r') as stream:
            try: 
                subsetconfig = yaml.safe_load(stream)                
            except:
                print ('Something went wrong reading ' + args.subsetconfig)            
    else:
        # DEFAULT
        subsetconfig = { 'subset': {'depth': {'min': 0, 'max': 5}, 'latitude': {'min': 14.26, 'max': 32.28}, 'longitude': {'min': -97.99, 'max': -75}, 'variables': ['water_u', 'water_v'], 'output': 'fnmoc-amseas-forecast-JK' }} 


    # La fecha del pronostico a descagar, por defecto usar la fecha actual.
    forecastDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")

    print ('Opening remote dataset..')
    remoteDataset = makeCFCompliant(dap2DataArray(
                                        getDapUrls(forecastDate),
                                        chunks={ 'depth' : 1 } ))


    # Definicion del subconjunto a descargar
    times = slice(0,remoteDataset.dims['time'])
    
    depths = getNearestIdxSlice(subsetconfig['subset']['depth']['min'], subsetconfig['subset']['depth']['max'], remoteDataset.coords['depth'].values)
    lats   = getNearestIdxSlice(subsetconfig['subset']['latitude']['min'], subsetconfig['subset']['latitude']['max'], remoteDataset.coords['lat'].values)
    lons   = getNearestIdxSlice(subsetconfig['subset']['longitude']['min'], subsetconfig['subset']['longitude']['max'], remoteDataset.coords['lon'].values)
    variables = subsetconfig['subset']['variables']
    
    # Con los parametros del subconjunto crear el DataArray con el recorte.
    subsets=[]
    ncFilenames=[]
    print ('Creating subsets: ', end='')
    for sidx in range(times.start, times.stop):        
        sidx2d = "{:02d}".format(sidx)
        print( sidx2d + ' ', end='' )
        subsets.append( remoteDataset[variables].isel(time=[sidx], depth=depths, lat=lats, lon=lons) )
        ncFilenames.append( getSafeOutputFilename(subsetconfig['subset']['output'] + '-' + forecastDate + '-time' + sidx2d , 'nc') )

    print ('Writing subsets to netcdf')
    try:
        delayedDownload = xr.save_mfdataset(subsets, ncFilenames, mode='w', compute=False, engine='netcdf4')
    except:
        print('h5netcdf package not installed, recommended for improved netcdf writing')
        delayedDownload = xr.save_mfdataset(subsets, ncFilenames, mode='w', compute=False, engine='netcdf4')

    with ProgressBar():
        result = delayedDownload.compute()    



'''

Test Descarga
 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 Writing subsets to netcdf
 [########################################] | 100% Completed |  8min  6.3s

TODO: 

 Test creating multiple subset datasets to use save_mfdataset in parallel mode with each subset
'''
