#!/usr/bin/env python

import os
import sys
import numpy as np
import xarray as xr
import yaml
import datetime as dt
from dask.diagnostics import ProgressBar
import argparse

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
    dst = xr.open_dataset('https://thredds.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best',
                          chunks=chunks, decode_times=False)
    return dst

def makeCFCompliant(dst):
    '''
    '''
    # Cordenadas 
    dst.coords['lon'] = dst.coords['lon'] - (180*2)   # Los datos de coordenadas para lon deben de ir de -180 a 0
                                                      # en unidades degrees_east
                                                          
    dst.coords['lon'].attrs['axis'] = 'X'
    dst.coords['lon'].attrs['standard_name'] = 'longitude'

    dst.coords['lat'].attrs['axis'] = 'Y'
    dst.coords['lat'].attrs['standard_name'] = 'latitude'

    dst.coords['height_above_ground4'].attrs['axis'] = 'Z'
    dst.coords['height_above_ground4'].attrs['standard_name'] = 'depth'

    dst.coords['time'].attrs['axis'] = 'T'
    dst.coords['time'].attrs['standard_name'] = 'time'

    dst.variables['u-component_of_wind_height_above_ground'].attrs['standard_name'] = 'x_wind'
    dst.variables['v-component_of_wind_height_above_ground'].attrs['standard_name'] = 'y_wind'
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
        subsetconfig = { 'subset': {'depth': {'min': 0, 'max': 5}, 'latitude': {'min': 14.26, 'max': 32.28}, 'longitude': {'min': -97.99, 'max': -75}, 'variables': ['u_component_of_wind_height_above_ground', 'v_component_of_wind_height_above_ground'], 'output': 'fnmoc-amseas-forecast' }} 

    forecastDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")

    print ('Opening remote GFS Dataset')
    remoteDataset = makeCFCompliant(getGFSDataArray())

    # Subset definition
    heights = getNearestIdxSlice(subsetconfig['subset']['height']['min'], subsetconfig['subset']['height']['max'], remoteDataset.coords['height_above_ground4'].values) 
    lats   = getNearestIdxSlice(subsetconfig['subset']['latitude']['min'], subsetconfig['subset']['latitude']['max'], remoteDataset.coords['lat'].values) 
    lons   = getNearestIdxSlice(subsetconfig['subset']['longitude']['min'], subsetconfig['subset']['longitude']['max'], remoteDataset.coords['lon'].values)
    variables = subsetconfig['subset']['variables']

    subset = remoteDataset[variables].isel(height_above_ground4=heights, lat=lats, lon=lons)

    # Escribir a disco el subconjunto seleccionado.
    ncFilename=getSafeOutputFilename('gfs-surface-winds-forecast-' + forecastDate, 'nc')
    print ('OutputFilename : ' + ncFilename)

    try:
        delayedDownload = xr.save_mfdataset([subset], ['./' + ncFilename], mode='w', compute=False, engine='h5netcdf')
    except:
        print('h5netcdf package not installed, recommended for improved netcdf writing')
        delayedDownload = xr.save_mfdataset([subset], ['./' + ncFilename], mode='w', compute=False, engine='netcdf4')

    with ProgressBar():
        result = delayedDownload.compute()      