#!/usr/bin/env python

import os
import sys
import yaml
import argparse
import datetime as dt
import netCDF4 as nc
# import xarray as xr
import numpy as np
from siphon.catalog import TDSCatalog
from subprocess import Popen
import pandas as pd


def getDapUrls(forecastDate):
    '''
     Devuelve el listado de urls ordenadas para consultar los datos del pronostico.
     https://www.ncei.noaa.gov/thredds-coastal/catalog/amseas/amseas_20201218_to_current/20220102/catalog.html
    '''
    amseasThreddsCatalogURL = \
    'https://www.ncei.noaa.gov/thredds-coastal/catalog/amseas/amseas_20201218_to_current/{}/catalog.xml'\
    .format(forecastDate)

    cat = TDSCatalog(amseasThreddsCatalogURL)
    dapURLs = [ dapurl.access_urls['OPENDAP'] for dapurl in cat.datasets.values() ] 
    dapURLs.reverse()
    return dapURLs

def buildNCKSDownload(dapURL, subsetConfig, outfile):
    '''
    ncks -v surf_el -d time,0,0 -d lat,15.0,32.0 -d lon,262.0,285.0 https://www.ncei.noaa.gov/thredds-coastal/dodsC/amseas/amseas_20201218_to_current/20211022/coamps_ncom_amseas_u_1_2021102200_00${ix}0000.nc  out_ssh_${ix}.nc;
             '-d', "lon,{minX},{maxX}".format(maxX=subsetConfig['longitude']['max']+360, minX=subsetConfig['longitude']['min']+360),
    '''

    return [ "ncks", "-O", "-v", ",".join([ "{}".format(var) for var in subsetConfig['variables'] ]),
             '-d', "lat,{minY},{maxY}".format(maxY=subsetConfig['latitude']['max'], minY=subsetConfig['latitude']['min']),
             '-d', "lon,{minX},{maxX}".format(maxX=subsetConfig['longitude']['max'], minX=subsetConfig['longitude']['min']),
             '-d', "depth,{minX},{maxX}".format(maxX=subsetConfig['depth']['max'], minX=subsetConfig['depth']['min']),
             dapURL, outfile ]

def values2NearestIdx(dapURL, subsetconfig):
    dst = nc.Dataset(dapURL)
    subsetconfig['latitude']['min'] = np.abs(dst.variables['lat'][:] - subsetconfig['latitude']['min']).argmin()
    subsetconfig['latitude']['max'] = np.abs(dst.variables['lat'][:] - subsetconfig['latitude']['max']).argmin()
    subsetconfig['longitude']['min'] = np.abs(dst.variables['lon'][:] - (subsetconfig['longitude']['min']+360)).argmin()
    subsetconfig['longitude']['max'] = np.abs(dst.variables['lon'][:] - (subsetconfig['longitude']['max']+360)).argmin()
    subsetconfig['depth']['min'] = np.abs(dst.variables['depth'][:] - subsetconfig['depth']['min']).argmin()
    subsetconfig['depth']['max'] = np.abs(dst.variables['depth'][:] - subsetconfig['depth']['max']).argmin()    
    dst.close()

def makeCFCompliant(ncFile):
    '''
     Agrega los atributos faltantes a los conjuntos de datos del pronostic fnmoc-amseas.
     Devuelve el DataArray con las modificaciones.
    '''
    # CF Compliant
    # Agregar los atributos necesarios al xarray.dataset que esta en memoria
    # para hacerlo "CF compliant"
    dst = nc.Dataset(ncFile,'a')

    # Cordenadas 
    dst.variables['lon'][:] = dst.variables['lon'][:] - 360.0   # Los datos de coordenadas para lon deben de ir de -180 a 0
                                                                # en unidades degrees_east
    dst.variables['lon'].axis = 'X'
    dst.variables['lon'].units = 'degrees_east'
    dst.variables['lon'].standard_name = 'longitude'

    dst.variables['lat'].axis = 'Y'
    dst.variables['lat'].standard_name = 'latitude'

    dst.variables['depth'].axis = 'Z'
    dst.variables['depth'].standard_name = 'depth'

    dst.variables['time'].axis = 'T'
    dst.variables['time'].standard_name = 'time'

    # Variables standard_names
    dst.variables['water_u'].standard_name = 'x_sea_water_velocity'
    dst.variables['water_v'].standard_name = 'y_sea_water_velocity'
    dst.variables['water_temp'].standard_name = 'sea_water_temperature'
    dst.variables['salinity'].standard_name = 'sea_water_salinity'
 
    dst.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parallel download for the FNMOC Amseas forecast', prog='download-fnmoc-amseas-forecast')
    parser.add_argument('--subset', '-s', action='store', dest='subsetconfig', help='yaml file with the subset to download.')
    parser.add_argument('--commands', '-c', action='store_true', dest='show_commands', help='Just show the ncks commands to run')

    # args = parser.parse_args('--config-file', "../subset_GoM.yaml")
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
        subsetconfig = { 'subset': {'depth': {'min': 0, 'max': 5000}, 'latitude': {'min': 14.26, 'max': 32.28}, 'longitude': {'min': -97.99 + 360, 'max': -75 + 360}, 'variables': ['water_u', 'water_v'], 'output': 'fnmoc-amseas-forecast-JKb' }} 

    # Check if only present day or period
    if subsetconfig['subset']['period']['sdate'] == None:
        # La fecha del pronostico a descagar, por defecto usar la fecha actual.
        # forecastDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")
        forecastDate = [(dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y%m%d")]
    else:
        sdate=subsetconfig['subset']['period']['sdate']
        edate=subsetconfig['subset']['period']['edate']
        sdate = dt.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
        edate = dt.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
        forecastDate = pd.date_range(sdate,edate).strftime("%Y%m%d")

    # Download each day
    for fDate in forecastDate:
        durls = getDapUrls(fDate)
        # Multiple curl calls in parallel
    
        # Convert lat,lon values to its nearest index
        values2NearestIdx(durls[0], subsetconfig['subset'])
    
        print ("Parallel downloading" )
        # outfiles = [ subsetconfig['subset']['output'] + '-' + forecastDate + '-time' + '{:02d}'.format(didx) + '.nc' for didx, dapURL in enumerate(durls) ] 
        # outfiles = [subsetconfig['subset']['outdir'] + 'fnmoc-amseas-' + subsetconfig['subset']['output'] + '-' + forecastDate + '-time' + '{:02d}'.format(didx) + '.nc' for didx, dapURL in enumerate(durls) ]
        os.makedirs(os.path.join(subsetconfig['subset']['outdir'],'fnmoc-amseas/',fDate), exist_ok=True)
        outfiles = [os.path.join(subsetconfig['subset']['outdir'],'fnmoc-amseas/',fDate) + '/fnmoc-amseas-' + subsetconfig['subset']['output'] + '-' + fDate + '-time' + '{:02d}'.format(didx) + '.nc' for didx, dapURL in enumerate(durls) ]
        ncksCommands = [ buildNCKSDownload(dapURL, subsetconfig['subset'], outfiles[didx] ) for didx, dapURL in enumerate(durls) ] 
    
        if args.show_commands:
            for cidx, c in enumerate(ncksCommands):
                print ( str(cidx) + " : " + " ".join(c) )
            sys.exit(0)
    
        concurrentDownloads=4
    
        print("Number of downloads: " + str(len(ncksCommands)))
        for i in range(0, len(ncksCommands), concurrentDownloads):
    
            print ("Batch " + str(i))
            downloadProceses = []
            for p in range(i, i+concurrentDownloads):
                if p >= len(ncksCommands):
                    break
                print("Running p: " + str(p))
                downloadProceses.append ( Popen(ncksCommands[p]) )
                
            # Wait for the downloading batch to finish.
            for p in downloadProceses:
                p.wait()
    
            # Show if the download process went well
            for pidx, p in enumerate(downloadProceses):
                print ("{} : {} : Return code: {}".format(pidx, ' '.join(ncksCommands[pidx]),  p.returncode))
    
            # Make downloaded files, CF Compliant
            for pidx, p in enumerate(downloadProceses):
                print("Making CF Compliant: " + outfiles[i+pidx])
                makeCFCompliant(outfiles[i+pidx])

        print(fDate + ' downloaded sucessfully...')