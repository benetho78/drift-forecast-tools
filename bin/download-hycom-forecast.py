#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 17:37:14 2022

@author: jcerda
"""
import os
import sys
import numpy as np
import xarray as xr
import yaml
import datetime as dt
from dask.diagnostics import ProgressBar
import argparse
import pandas as pd
import cftime

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
    dst = xr.open_dataset('https://tds.hycom.org/thredds/dodsC/GOMu0.04/expt_90.1m000/FMRC/GOMu0.04_901m000_FMRC_best.ncd',
                          chunks=chunks, decode_times=False)
    return dst

def makeCFCompliant(dst):
    ''''
    '''
    # Cordenadas 
    dst.coords['lon'] = dst.coords['lon']   # Los datos de coordenadas para lon deben de ir de -180 a 0
                                            # en unidades degrees_east
                                                          
    dst.coords['lon'].attrs['axis'] = 'X'
    dst.coords['lon'].attrs['standard_name'] = 'longitude'
    dst.coords['lon'].attrs['units'] = 'degrees_east'

    dst.coords['lat'].attrs['axis'] = 'Y'
    dst.coords['lat'].attrs['standard_name'] = 'latitude'
    dst.coords['lat'].attrs['units'] = 'degrees_north'

    dst.coords['depth'].attrs['axis'] = 'Z'
    dst.coords['depth'].attrs['standard_name'] = 'depth'

    dst.coords['time'].attrs['axis'] = 'T'
    dst.coords['time'].attrs['standard_name'] = 'time'
    dst.coords['time'] = cftime.num2date(dst.time, dst.time.attrs['units'])

    # Variables standard_names
    dst.variables['water_u'].attrs['standard_name'] = 'x_sea_water_velocity'
    dst.variables['water_v'].attrs['standard_name'] = 'y_sea_water_velocity'
    dst.variables['water_temp'].attrs['standard_name'] = 'sea_water_temperature'
    dst.variables['salinity'].attrs['standard_name'] = 'sea_water_salinity'

    return dst


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Download for the HYCOM', prog='download-hycom-forecast')
    parser.add_argument('--subset', '-s', action='store', dest='subsetconfig', help='yaml file with the subset to download.')

    args = parser.parse_args()
    # TODO Add verbose mode
    if args.subsetconfig:
        with open(args.subsetconfig, 'r') as stream:
            try: 
                subsetconfig = yaml.safe_load(stream)                
            except:
                print ('Something went wrong reading ' + args.subsetconfig)            
    # else:
    #     # DEFAULT
    #     subsetconfig = { 'subset': {'height': {'min': 0, 'max': 20}, 'latitude': {'min': 14.26, 'max': 32.28}, 'longitude': {'min': -97.99, 'max': -75}, 'variables': ['u-component_of_wind_height_above_ground', 'v-component_of_wind_height_above_ground'], 'output': 'gfs-winds-forecast' }} 

    # if subsetconfig['subset']['period']['sdate'] == None:
    forecastDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")
    fDate = (dt.datetime.today().replace(microsecond=0, second=0, minute=0, hour=0) - dt.timedelta(days=1))
    # else:
    #     sdate=subsetconfig['subset']['period']['sdate']
    #     edate=subsetconfig['subset']['period']['edate']
    #     sdate = dt.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
    #     edate = dt.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
    #     forecastDate = pd.date_range(sdate,edate).strftime("%Y%m%d")


    print ('Opening remote HYCOM Dataset')
    remoteDataset = makeCFCompliant(getGFSDataArray())

    # Subset definition
    heights = getNearestIdxSlice(subsetconfig['subset']['depth']['min'], subsetconfig['subset']['depth']['max'], remoteDataset.coords['depth'].values) 
    print (heights)
    lats   = getNearestIdxSlice(subsetconfig['subset']['latitude']['min'], subsetconfig['subset']['latitude']['max'], remoteDataset.coords['lat'].values) 
    lons   = getNearestIdxSlice(subsetconfig['subset']['longitude']['min'], subsetconfig['subset']['longitude']['max'], remoteDataset.coords['lon'].values)

    fD = cftime.DatetimeGregorian(fDate.year, fDate.month, fDate.day, fDate.hour)
    # times=slice((remoteDataset.coords['time'].values == fD).argmax(), len(remoteDataset.coords['time']))
    t0 = (remoteDataset.coords['time'].values == fD).argmax()
    # # Solo 24 hrs
    # t1 = t0 + (24*1)
    # Desde el dia de antier hasta el ultimo dia de forecast
    t1 = remoteDataset.coords['time'].size
    times=slice(t0, t1)
    
    variables = subsetconfig['subset']['variables']

    subset = remoteDataset[variables].isel(time=times, depth=heights, lat=lats, lon=lons)

    # Escribir a disco el subconjunto seleccionado.
    # ncFilename=getSafeOutputFilename(subsetconfig['subset']['output'] + forecastDate, 'nc')
    ncFilename=getSafeOutputFilename(subsetconfig['subset']['outdir'] + 'hycom/' + 'HYCOM-' + subsetconfig['subset']['output'] + '-' + forecastDate, 'nc')
    print ('OutputFilename : ' + ncFilename)

    try:
        delayedDownload = xr.save_mfdataset([subset], [ncFilename], mode='w', compute=False, engine='h5netcdf')
    except:
        print('h5netcdf package not installed, recommended for improved netcdf writing')
        delayedDownload = xr.save_mfdataset([subset], [ncFilename], mode='w', compute=False, engine='netcdf4')

    with ProgressBar():
        result = delayedDownload.compute()      
