#!/usr/bin/env python

import os
import sys
import numpy as np
import xarray as xr
import yaml
import datetime as dt
from dask.diagnostics import ProgressBar
import argparse
import pandas as pd

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

def getGFSDataArray(chunks={'time' : 1}):
    # dst = xr.open_dataset('https://thredds.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best',
    #                       chunks=chunks, decode_times=False)
    dst = xr.open_dataset('https://thredds.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best',
                          chunks=chunks)
    # dst = xr.open_dataset('https://thredds-dev.unidata.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best',
    #                       chunks=chunks)
    return dst

def makeCFCompliant(dst):
    '''
    '''
    # Cordenadas 
    dst.coords['lon'] = dst.coords['lon'] - (180*2)   # Los datos de coordenadas para lon deben de ir de -180 a 0
                                                      # en unidades degrees_east
                                                          
    dst.coords['lon'].attrs['axis'] = 'X'
    dst.coords['lon'].attrs['standard_name'] = 'longitude'
    dst.coords['lon'].attrs['units'] = 'degrees_east'

    dst.coords['lat'].attrs['axis'] = 'Y'
    dst.coords['lat'].attrs['standard_name'] = 'latitude'
    dst.coords['lat'].attrs['units'] = 'degrees_north'

    dst.coords['height_above_ground2'].attrs['axis'] = 'Z'
    dst.coords['height_above_ground2'].attrs['standard_name'] = 'depth'

    dst.coords['time'].attrs['axis'] = 'T'
    dst.coords['time'].attrs['standard_name'] = 'time'

    dst.variables['u-component_of_wind_height_above_ground'].attrs['standard_name'] = 'x_wind'
    dst.variables['v-component_of_wind_height_above_ground'].attrs['standard_name'] = 'y_wind'
    return dst


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Download for the GFS', prog='download-gfs-forecast')
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
        subsetconfig = { 'subset': {'height': {'min': 0, 'max': 20}, 'latitude': {'min': 14.26, 'max': 32.28}, 'longitude': {'min': -97.99, 'max': -75}, 'variables': ['u-component_of_wind_height_above_ground', 'v-component_of_wind_height_above_ground'], 'output': 'gfs-winds-forecast' }} 

    if subsetconfig['subset']['period']['sdate'] == None:
        forecastDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")
    else:
        sdate=subsetconfig['subset']['period']['sdate']
        edate=subsetconfig['subset']['period']['edate']
        sdate = dt.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
        edate = dt.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
        forecastDate = pd.date_range(sdate,edate).strftime("%Y%m%d")


    print ('Opening remote GFS Dataset')
    remoteDataset = makeCFCompliant(getGFSDataArray())

    # Subset definition
    heights = getNearestIdxSlice(subsetconfig['subset']['height']['min'], subsetconfig['subset']['height']['max'], remoteDataset.coords['height_above_ground2'].values) 
    print (heights)
    lats   = getNearestIdxSlice(subsetconfig['subset']['latitude']['max'], subsetconfig['subset']['latitude']['min'], remoteDataset.coords['lat'].values) 
    lons   = getNearestIdxSlice(subsetconfig['subset']['longitude']['min'], subsetconfig['subset']['longitude']['max'], remoteDataset.coords['lon'].values)
    # variables = subsetconfig['subset']['variables']
    variables = subsetconfig['subset']['variablesGFS']

    subset = remoteDataset[variables].isel(height_above_ground2=heights, lat=lats, lon=lons)

    # Escribir a disco el subconjunto seleccionado.
    # ncFilename=getSafeOutputFilename(subsetconfig['subset']['output'] + forecastDate, 'nc')
    ncFilename=getSafeOutputFilename(subsetconfig['subset']['outdir'] + 'gfs-winds/' + 'gfs-winds-' + subsetconfig['subset']['output'] + '-' + forecastDate, 'nc')    
    print ('OutputFilename : ' + ncFilename)

    try:
        delayedDownload = xr.save_mfdataset([subset], [ncFilename], mode='w', compute=False, engine='h5netcdf')
    except:
        print('h5netcdf package not installed, recommended for improved netcdf writing')
        delayedDownload = xr.save_mfdataset([subset], [ncFilename], mode='w', compute=False, engine='netcdf4')

    with ProgressBar():
        result = delayedDownload.compute()      
