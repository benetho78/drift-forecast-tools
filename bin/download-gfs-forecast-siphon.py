#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 12:56:55 2022

@author: jcerda
"""
from siphon.catalog import TDSCatalog
from datetime import datetime
from xarray.backends import NetCDF4DataStore
import xarray as xr
import numpy as np
from netCDF4 import num2date
from metpy.units import units
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from metpy.plots import ctables

#%% Helper function for finding proper time variable
def find_time_var(var, time_basename='time'):
    for coord_name in var.coords:
        if coord_name.startswith(time_basename):
            return var.coords[coord_name]
    raise ValueError('No time variable found for ' + var.name)


#%%
best_gfs = TDSCatalog('http://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/'
                      'Global_0p25deg/catalog.xml?dataset=grib/NCEP/GFS/Global_0p25deg/Best')

best_ds = list(best_gfs.datasets.values())[0]
ncss = best_ds.subset()
ncss.variables

query = ncss.query()
# query.lonlat_box(north=32.28, south=9, east=-75, west=-98).time(datetime.utcnow())
query.lonlat_box(north=32.28, south=9, east=360-75, west=360-98).time(datetime.utcnow())
query.accept('netcdf4')
query.variables('u-component_of_wind_height_above_ground','v-component_of_wind_height_above_ground')

data = ncss.get_data(query)
data = xr.open_dataset(NetCDF4DataStore(data))
list(data)

U_4d = data['u-component_of_wind_height_above_ground']
V_4d = data['v-component_of_wind_height_above_ground']

time_1d = find_time_var(U_4d)
lon_1d = data['longitude']
lat_1d = data['latitude']

# Reduce the dimensions of the data and get as an array with units
# U_2d = U_3d.metpy.unit_array.squeeze()
# V_2d = V_3d.metpy.unit_array.squeeze()
U_2d = U_4d[0,0,:,:].metpy.unit_array.squeeze()
V_2d = V_4d[0,0,:,:].metpy.unit_array.squeeze()
# Combine latitude and longitudes 
lon_2d, lat_2d = np.meshgrid(lon_1d, lat_1d)

#%% Create a new figure
fig = plt.figure(figsize=(20, 20))
# Add the map and set the extent
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([260, 287, 5, 35])
# # Retrieve the state boundaries using cFeature and add to plot
# ax.add_feature(cfeature.STATES, edgecolor='gray')
# Retrieve the state boundaries using cFeature and add to plot
ax.add_feature(cfeature.BORDERS, edgecolor='gray')
ax.add_feature(cfeature.COASTLINE, edgecolor='gray')
# # Contour temperature at each lat/long
# contours = ax.contourf(lon_2d, lat_2d, U_2d.to('km/h'), 1, transform=ccrs.PlateCarree(),
#                        cmap='RdBu_r')
# Contour temperature at each lat/long
contours = ax.contourf(lon_2d, lat_2d, U_2d, 20, transform=ccrs.PlateCarree(),
                       cmap='turbo')

#Plot a colorbar to show temperature and reduce the size of it
fig.colorbar(contours)
# # Make a title with the time value
# ax.set_title(f'Temperature forecast (\u00b0F) for {time_1d[0].values}Z', fontsize=20)
# Make a title with the time value
ax.set_title(f'U-vel forecast (m/s) for {time_1d[0].values}Z', fontsize=20)

# Plot markers for each lat/long to show grid points for 0.25 deg GFS
ax.plot(lon_2d.flatten(), lat_2d.flatten(), linestyle='none', marker='o',
        color='black', markersize=2, alpha=0.3, transform=ccrs.PlateCarree())

plt.savefig('//home//jcerda//Python//cicoil-tools//Hola.png')
