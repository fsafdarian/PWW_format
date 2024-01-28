"""
Created on Fri Nov 24 14:18:47 2023

@authors: Thomas chen, Lyric Haylow and Farnaz Safdarian
Purpose: To translate .nc files to .pww or .csv given any coords
"""
import os
import glob
import re
import json
import cdsapi
import yaml
import time
import xarray as xr
import pandas as pd
import numpy as np
from datetime import date
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import netCDF4
import multiprocessing as mp
from tqdm import tqdm
import netCDF4
import logging
import dask.dataframe as dd

logger = logging.getLogger("ERA5_downloader")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    ### Different Coordinates ###
    # North America, Colombia, some Greenland ([80, -170, 0, -30])
    # USA Only, ([58, -130, 24, -60])
    area = [80, -170, 0, -30] # Options: US, Texas, or spec coords like [80, -170, 0, -30]
    startDate = datetime.date(2008, 1, 1)  # Beginning of what to run   ex: 2021, 1, 1
    endDate = datetime.date(2008, 3, 1)    # End of what to run         ex: 2021, 5, 1 NOTE: does not get 5, just till 4

    cds: CDSWeather = CDSWeather(output_file_path=".", stime = startDate, etime = endDate, area=area)
    cds.download_data(period=datetime.timedelta(365), post_process=False, chunk_size=1)

class CDSWeather:
    # [58, -130, 24, -60] USA
    def __init__(self, stime=datetime.date(2021, 1, 1), etime=datetime.date(2021, 2, 1), area=[58, -130, 24, -60],
                 output_file_path=''):
        self.stime = stime
        self.etime = etime
        self.area = area
        self.output_path = output_file_path
        self.api_key = "197254:80c8ec34-7bd2-4a00-86f6-488629cb2ee1" # replace with your own api #197254:80c8ec34-7bd2-4a00-86f6-488629cb2ee1
        self.url = "https://cds.climate.copernicus.eu/api/v2"
        self.hours = ['{:02d}:00'.format(h) for h in range(24)]
        self.days = ['{:02d}'.format(d) for d in range(1, 32)]
        self.moths = ['{:02d}'.format(m) for m in range(1, 13)]
        self.path_validation()

    def path_validation(self):
        if not os.path.exists(self.output_path + '/nc'):
            os.makedirs(self.output_path + '/nc')
        if not os.path.exists(self.output_path + '/csv1'):
            os.makedirs(self.output_path + '/csv1')

    def get_weather_data(self, dates, output_file_name='weather_data.nc'):
        """
        The data will be downloaded for monthly basis for entire year of data
        Input:
             file_location: Strings
        Outputs:
            file_location: A string as in input
        """
        c = cdsapi.Client(key=self.api_key, url=self.url, debug=False, progress=False)
        unique = lambda l: list(set(l))
        year = [d.year for d in dates]
        month = [d.month for d in dates]
        # Create a list of all the years, months, days and hours we want to download
        days = [d.day for d in dates]
        try:
            c.retrieve(
                'reanalysis-era5-single-levels', {
                    'product_type': 'reanalysis',  # This is the dataset produced by the CDS
                    'variable': ['2m_dewpoint_temperature', '2m_temperature', 
                                '100m_u_component_of_wind', '100m_v_component_of_wind', '10m_u_component_of_wind', 
                                '10m_v_component_of_wind', 'total_cloud_cover', 'high_cloud_cover', 'low_cloud_cover', 
                                'medium_cloud_cover', 'surface_solar_radiation_downwards', 
                                'total_sky_direct_solar_radiation_at_surface', 'geopotential'],
                    'year': unique(year),
                    'month': unique(month),
                    'day': unique(days),
                    'area': self.area,
                    'time': self.hours,  # default is all hours
                    'format': 'netcdf'  # The nc formate
                },
                output_file_name)
            print('retrieved')

        except Exception as e:
            print(f'Failed to download data for {output_file_name}...\n{e}, \n retrying with different variables')
            return -1
        return 0
    
    def download_data(self, period=datetime.timedelta(days=10), post_process=False, chunk_size=1, memory_limit=10):
        """ base on the start date and end date download spread files using multithreading
        Input:
            interval: 'day' or 'month'
            period: datetime.timedelta(days=10) or datetime.timedelta(months=1)
            freq: 'D' or 'M'
            memory_limit: 10 GB
        Output:
        """
        # !TODO make it work with whatever frequency
        executor = ThreadPoolExecutor(max_workers=6)
        thread_pool = []
        paths = []

        def split_date(dates, chunk_size):
            x = 0
            new_dates = []
            temp_dates = []
            for i in range(len(dates)):
                x += 1
                temp_dates.append(dates[i])
                # the 3 months is the maximum server would permit
                if (x >= chunk_size) | (i >= len(dates) - 1) | (dates[i].month == 12) | (x >= 3):
                    new_dates.append(temp_dates)
                    temp_dates = []
                    x = 0
            return new_dates
        print('Downloading data')
        '''----------------------------------- Download data for each month -----------------------------------'''

        days = pd.date_range(self.stime, self.etime, freq="M")  # download data on the sample frequency
        days = split_date(days, chunk_size)
        print(days)
        for d in days:
            d = pd.DatetimeIndex(d)
            output_file_name = f"{d[0].strftime('%Y%m')}.nc" if chunk_size == 1 else \
                f"{d[0].strftime('%Y%m')}_{d[-1].strftime('%Y%m')}.nc"
            output_file_name = os.path.join(self.output_path + '/nc', output_file_name)
            thread_pool.append(executor.submit(self.get_weather_data, d, output_file_name))
            paths.append([output_file_name, d])

if __name__ == '__main__':
    main()