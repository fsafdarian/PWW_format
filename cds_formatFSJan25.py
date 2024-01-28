"""
Created on Thu Oct 12 15:22:47 2023

@authors: Thomas chen, Lyric Haylow and Farnaz Safdarian
Purpose: To streamline cds_reading.py to have only the format 'nc_to_df' function available
"""
import os
import glob
import re
import json
import struct
import pytz
import yaml
import time
import xarray as xr
import pandas as pd
import numpy as np
from datetime import date, datetime
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
from tqdm import tqdm
import netCDF4
import logging
import dask.dataframe as dd

logger = logging.getLogger("ERA5_downloader")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    ### Conditionals ###
    # Whether to download or use prior
    # What area to use, whether US, Texas, or specific. 
    # what kind of files to save, csv pww 

    area = [80, -170, 0, -30]
    cds: CDSWeather = CDSWeather(output_file_path=".")
    cds.process_data(file_type='nc')

def get_file(path, freq='Y'):
    """ get all the files in the file_path and return the date of each file
    @param path: the path of the file with extension ie: 'C:/Users/.../*.nc'
    @param freq: the frequency of the data, 'y' for yearly and 'm' for monthly
    :return: files, Date (list of date and index of each file)
    """
    files = np.array(glob.glob(path))  # get all of files
    Date = []
    for i, file in enumerate(tqdm(files, desc="extract date from files…", ncols=100, unit="file")):  # list of file
        if freq == 'M':  # for monthly data
            raw_date = re.search(r"([12]\d{3})(0[1-9]|1[0-2])", os.path.basename(file))
            if raw_date: Date.append(date(int(raw_date[1]), int(raw_date[2]), 1))
        else:  # for yearly data
            raw_date = re.search(r"[1-3][0-9]{3}", os.path.basename(file))
            if raw_date: Date.append(date(int(raw_date[0]), 1, 1))  # for yearly data
    return files, Date

def nc_to_df(df=None, nc_file=None, estimate_wind=False, engine="xarray", to_disk=False):
    # columns for running successfully: ['u100', 'v100', 'u10', 'v10', 'd2m', 't2m', 'z', 'hcc', 'lcc', 'mcc', 'ssrd', 'tcc', 'fdir']
    """ Convert a netcdf file to PW aux/axd data section.
    :ivar
    df: the dataframe to be converted(if None, read from nc_file)
    nc_file: the netcdf file to be converted
    takeout estimate_wind: if True, estimate the wind speed from 10m to 100m
    engine: the engine to read the netcdf file, "xarray" or "pd"
    takeout to_disk: if True, save the dataframe to disk
    :return: df: the converted dataframe
    """
    def to_str(x, lens):
        x_str = "{:.2f}".format(x)
        if (x // 100) == 0:
            # lon=format(lon, '.2f')
            return x_str.zfill(lens)
        elif ((-x) // 100) == 0:
            # lon = format(lon, '.2f')
            return x_str.zfill(lens + 1)
        else:
            return x_str

    if df is None:
        ds = xr.open_dataset(nc_file)
        df = ds.to_dataframe()
    # flat the dataset for pd
    print(df.columns)

    if engine == "pd":
        df = df.to_dataframe()
        df.reset_index(inplace=True)
        df['station_id'] =    '+' + df['latitude'].apply(to_str, args=(5,))+df['longitude'].apply(to_str, args=(6,))+'/'
    df['sped'] = np.sqrt(df['u10'] ** 2 + df['v10'] ** 2)
    df['sped100'] = np.sqrt(df['u100'] ** 2 + df['v100'] ** 2)
    df['drct'] = np.arctan2(df['u10'], df['v10'])
    """----------------------------recover the missing data------------------"""
    if engine == "pd":
        df['drct'].where(df['drct'].notna(), np.arctan2(df['u10'], df['v10']), inplace=True)
        df['tcc'].where(df['tcc'].notna(), (df['hcc'] + df['mcc'] + df['lcc']) / 3, inplace=True)
    else:
        df['drct'].where(df['drct'].isnull(), np.arctan2(df['u10'], df['v10']))
        df['tcc'].where(df['tcc'].isnull(), (df['hcc'] + df['mcc'] + df['lcc']) / 3)
    # unit conversion
    df['t2m'] = np.round((df['t2m'] - 273.15) * 9 / 5 + 32)  # convert to degF
    df['d2m'] = np.round((df['d2m'] - 273.15) * 9 / 5 + 32)  # convert to degF
    df['sped'] = np.round(df['sped'] * 2.23694)  #convert from mps to mph
    df['tcc'] = np.round(df['tcc'] * 100)  # convert to %
    df['drct'] = np.round(df['drct'] * 180 / np.pi + 180)  # convert to deg
    # ERA5 data their values are given in J/m^2
    # Since the assumed period is 3600 seconds, just divide by 3600 to convert to W/m^2.
    df['ssrd'] = df['ssrd']/3600 
    # print(df['ssrd'])
    # quit(1)
    df['fdir'] = df['fdir']/3600
    if 'z' in df.columns:
        df['z'] = df['z']/9.81
    df['WindSpeed100mph'] = df['sped100'] * 2.23694  #convert from mps to mph
    if engine != "pd":
        df.reset_index(inplace=True)
        df['station_id'] =    '+' + df['latitude'].apply(to_str, args=(5,))+df['longitude'].apply(to_str, args=(6,))+'/'
        # formate to PW auxiliary file
    if 'z' in df.columns:
        df.drop(columns=['longitude', 'latitude', 'u100', 'sped100', 'z', 'v100', 'u10', 'v10', 'hcc', 'mcc', 'lcc'], inplace=True)
    else:
        df.drop(columns=['longitude', 'latitude', 'u100', 'sped100', 'v100', 'u10', 'v10', 'hcc', 'mcc', 'lcc'], inplace=True)
    # convert to PW parameters
    df['time'] = np.datetime_as_string(df['time'], unit='ms', timezone='UTC')
    df.rename( 
        columns={"time": "UTCISO8601", "station_id": "WhoAmI", 't2m': 'tempF', 'd2m': 'DewPointF',
                 "sped": "WindSpeedmph",
                 "drct": "WindDirection",
                 'tcc': 'CloudCoverPerc',
                 'ssrd': 'GlobalHorizontalIrradianceWM2',
                 'fdir': 'DirectHorizontalIrradianceWM2'}, inplace=True)
    # u100n from u100 and v100
    # print(df['GlobalHorizontalIrradianceWM2'])
    # print(type(df['GlobalHorizontalIrradianceWM2'].unique()))
    df_new_columnlist = ['UTCISO8601','WhoAmI','DewPointF','tempF', 'GlobalHorizontalIrradianceWM2','CloudCoverPerc','DirectHorizontalIrradianceWM2','WindSpeedmph','WindDirection','WindSpeed100mph']
    df = df.reindex(columns = df_new_columnlist)
    df = df.sort_values(by=['UTCISO8601'])
    df['UTCISO8601'] = np.where(df['UTCISO8601'].duplicated(), '', df['UTCISO8601'])
    # Fixes Global column for nans and spaces
    df.GlobalHorizontalIrradianceWM2.fillna(method='ffill', inplace=True)
    # Fixed Direct column for nans and spaces
    df.DirectHorizontalIrradianceWM2.fillna(method='ffill', inplace=True)
    df = df.fillna('')

    try:
        df = df.astype(
            {'WhoAmI': 'str', 'tempF': 'int16', 'DewPointF': 'int16', 'WindSpeedmph': 'int8', 'WindDirection': 'int16',
            'CloudCoverPerc': 'int8', 'GlobalHorizontalIrradianceWM2':'int16', 'DirectHorizontalIrradianceWM2':'int16', 'WindSpeed100mph':'int16'})
    except ValueError:
            try:
                # Fixes Global column for nans and spaces
                df.GlobalHorizontalIrradianceWM2.fillna(method='ffill', inplace=True)
                # Fixed Direct column for nans and spaces
                df.DirectHorizontalIrradianceWM2.fillna(method='ffill', inplace=True)
            except ValueError as e:
                print('Issue: not in Global or Direct columns.')
                print(e)
                quit(1)
    except:
        print('Issue: not a ValueError.')
        quit(1)
    print('h5')
    return df

# Define the conversion function # Excel's epoch starts on "January 0, 1900"
def datetime_to_excel_double(date_obj):
    excel_epoch = datetime(1899, 12, 31, tzinfo=pytz.UTC)  # Make it timezone-aware by setting the timezone to UTC
    delta = date_obj - excel_epoch
    excel_date = delta.days + (delta.seconds / 86400.0) + (date_obj.microsecond / 86400e6)+1
    return excel_date


def df_to_pww(df, file_name):
    def to_str(x, lens):
        x_str = "{:.2f}".format(x)
        if (x // 100) == 0:
            # lon=format(lon, '.2f')
            return x_str.zfill(lens)
        elif ((-x) // 100) == 0:
            # lon = format(lon, '.2f')
            return x_str.zfill(lens + 1)
        else:
            return x_str
    
    ### Loads in station.csv ### 
    df_station=pd.read_csv("stationLatLong.csv")
    df_station['Region']=df_station['Region'].fillna('')
    df_station['Country2']=df_station['Country2'].fillna('')
    df_station['ElevationMeters']=df_station['ElevationMeters'].astype(int)
    df_station['Region']=df_station['Region'].astype(str)
    df_station['Country2']=df_station['Country2'].astype(str)
    #df_station.reset_index(inplace=True)
    #df_station['WhoAmI'] = df_station['Latitude'].apply(to_str, args=(6,)) + '_' + df_station['Longitude'].apply(to_str, args=(5,))
   # df_station['WhoAmI'].drop_duplicates(inplace=True)
    #df_station = df_station.sort_values(by=['WhoAmI'])
    #LOC=df_station['WhoAmI'].nunique()

    # Define the conversion function # Excel's epoch starts on "January 0, 1900"
    def datetime_to_excel_double(date_obj):
        excel_epoch = datetime(1899, 12, 31, tzinfo=pytz.UTC)  # Make it timezone-aware by setting the timezone to UTC
        delta = date_obj - excel_epoch
        excel_date = delta.days + (delta.seconds / 86400.0) + (date_obj.microsecond / 86400e6)+1
        return excel_date

    # Fill NaN values with the previous non-null value
    df['UTCISO8601'] = df['UTCISO8601'].fillna(method='ffill')

    df = df.fillna(method='ffill') #just for 197605
    # Convert UTCISO8601 to timestamp
    df['UTCISO8601'] = pd.to_datetime(df['UTCISO8601'])
    df['excel_double_datetime'] = df['UTCISO8601'].apply(datetime_to_excel_double)
    df = df.sort_values(by=['excel_double_datetime', 'WhoAmI'])
    COUNT=df['excel_double_datetime'].nunique()
    unique_dates = df['UTCISO8601'].unique()
    df['tempF102'] = df['tempF'] + 115
    df['DewPointF104'] = df['DewPointF'] + 115
    df['WindDirection107'] = df['WindDirection'] /5
    df['GlobalHorizontalIrradianceWM2_120'] = df['GlobalHorizontalIrradianceWM2'] /5
    df['DirectHorizontalIrradianceWM2_121'] = df['DirectHorizontalIrradianceWM2'] /5
    df['WindDirection107'] = df['WindDirection107'].astype(int)
    df['GlobalHorizontalIrradianceWM2_120'] = df['GlobalHorizontalIrradianceWM2_120'].astype(int)
    df['DirectHorizontalIrradianceWM2_121'] = df['DirectHorizontalIrradianceWM2_121'].astype(int)
    df['tempF102']=df['tempF102'].astype(int) #just for 197605
    df['WindSpeedmph']=df['WindSpeedmph'].astype(int) #just for 197605
    df['CloudCoverPerc']=df['CloudCoverPerc'].astype(int) #just for 197605
    df['WindSpeed100mph']=df['WindSpeed100mph'].astype(int) #just for 197605
    #df['GlobalHorizontalIrradianceWM2_120']=df['GlobalHorizontalIrradianceWM2_120'].fillna(method='ffill')

    # Extract filename without extension
    file_name_without_extension = os.path.splitext(file_name)[0]  

    def to_ascii_null_terminated(s):
        try:
            # Encode to ASCII and add a null terminator
            return s.encode('ascii') + b'\x00'
        except UnicodeEncodeError:
            # Handle the case where the string cannot be encoded in ASCII
            # This will replace non-ASCII characters with '?', and then add a null terminator
            return s.encode('ascii', 'replace') + b'\x00'
        
    df_station['ascii_null_terminated_WhoAmI'] = df_station['WhoAmI'].apply(to_ascii_null_terminated)   
    df_station['ascii_null_terminated_Region'] = df_station['Region'].apply(to_ascii_null_terminated) 
    df_station['ascii_null_terminated_Country2'] = df_station['Country2'].apply(to_ascii_null_terminated) 

    aPWWVersion = 1
    aPWWFileName = file_name_without_extension + ".pww"  

    # Extracting the smallest start time and the largest end time from the DataFrame and converting them to timestamps
    aStartDateTimeUTC = df['excel_double_datetime'].min()
    aEndDateTimeUTC = df['excel_double_datetime'].max()
    #area=[58, -130, 24, -60] North 58°, West -130°, South 24°, East -60°  
    aMinLat=-130
    aMaxLat=-60
    aMinLon=24
    aMaxLon=58
    # Define the optional identifier field count
    LOC_FC = 0  # for extra loc variables from table 1
    VARCOUNT = 8  # Set this to the number of weather variable types you have

    with open(aPWWFileName, 'wb') as file:
        file.write(struct.pack('<h', 2001))
        file.write(struct.pack('<h', 8065))
        file.write(struct.pack('<h', aPWWVersion))
        file.write(struct.pack('<d', aStartDateTimeUTC))
        file.write(struct.pack('<d', aEndDateTimeUTC))
        file.write(struct.pack('<d', aMinLat))
        file.write(struct.pack('<d', aMaxLat))
        file.write(struct.pack('<d', aMinLon))
        file.write(struct.pack('<d', aMaxLon))
        file.write(struct.pack('<h', 0))
        file.write(struct.pack('<i', COUNT))    #countNumber of datetime values (COUNT)
        file.write(struct.pack('<i', 3600))
        file.write(struct.pack('<i', LOC)) #Number of weather measurement locations (LOC)
        file.write(struct.pack('<h', LOC_FC)) #Loc_FC # Pack the data into INT16 format and write to stream
        file.write(struct.pack('<h', VARCOUNT))

        # Temp in F
        file.write(struct.pack('<h', 102))
        # Dew point in F
        file.write(struct.pack('<h', 104))
        # Wind speed at surface (10m) in mph
        file.write(struct.pack('<h', 106))
        # Wind direction at surface (10m) in 5-degree increments
        file.write(struct.pack('<h', 107))
        # Total cloud cover percentage
        file.write(struct.pack('<h', 119))
        # Wind speed at 100m in mph
        file.write(struct.pack('<h', 110))
        # Global Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack('<h', 120))
        # Direct Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack('<h', 121))
        file.write(struct.pack('<h', 8)) #BYTECOUNT

        for row in df_station.index:
            # Write Latitude (DOUBLE)
            file.write(struct.pack('<d', df_station['Latitude'][row]))
            # Write Longitude (DOUBLE)
            file.write(struct.pack('<d', df_station['Longitude'][row])) 
            # Write AltitudeM (INT16)
            file.write(struct.pack('<h', df_station['ElevationMeters'][row]))
            # Write Name (STRING)
            file.write(df_station['ascii_null_terminated_WhoAmI'][row])
            file.write(df_station['ascii_null_terminated_Country2'][row])
            file.write(df_station['ascii_null_terminated_Region'][row])

        for date in unique_dates:
            # Filter rows by unique date
            rows = df[df['UTCISO8601'] == date]
            
            for temp in rows['tempF102']:
                #file.write(temp.encode('utf-8'))
                #file.write(struct.pack('<i', temp))
                file.write(temp.to_bytes(1, 'little'))
            
            for dew_point in rows['DewPointF104']:
                #file.write(struct.pack('<b', dew_point.to_bytes(1, 'little')))
                file.write(dew_point.to_bytes(1, 'little'))
            for wind_speed in rows['WindSpeedmph']:
                #file.write(struct.pack('<b', wind_speed.to_bytes(1, 'little')))
                file.write(wind_speed.to_bytes(1, 'little'))
        
            for wind_direction in rows['WindDirection107']:
                #file.write(struct.pack('d', wind_direction))
                file.write(wind_direction.to_bytes(1, 'little'))
            
            for CloudCoverPerc in rows['CloudCoverPerc']:
                #file.write(struct.pack('<b', CloudCoverPerc.to_bytes(1, 'little')))
                file.write(CloudCoverPerc.to_bytes(1, 'little'))
            
            for WindSpeed100mph in rows['WindSpeed100mph']:
                #file.write(struct.pack('<b', WindSpeed100mph.to_bytes(1, 'little')))
                file.write(WindSpeed100mph.to_bytes(1, 'little'))
            
            for GlobalHorizontalIrradianceWM2_120 in rows['GlobalHorizontalIrradianceWM2_120']:
                #file.write(struct.pack('d', GlobalHorizontalIrradianceWM2_120))
                file.write(GlobalHorizontalIrradianceWM2_120.to_bytes(1, 'little'))
            
            for DirectHorizontalIrradianceWM2_121 in rows['DirectHorizontalIrradianceWM2_121']:
                #file.write(struct.pack('d', DirectHorizontalIrradianceWM2_121))
                file.write(DirectHorizontalIrradianceWM2_121.to_bytes(1, 'little'))
    del df
    del df_station
    file.close() 
    

class CDSWeather:
    def __init__(self, area=[80, -170, 0, -30], output_file_path=''):
        self.area = area
        self.output_path = output_file_path
        self.api_key = '197254:80c8ec34-7bd2-4a00-86f6-488629cb2ee1' # '248754:cceabf43-9a89-4727-806a-8dab7d516c75'
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
        if not os.path.exists(self.output_path + '/aux1'):
            os.makedirs(self.output_path + '/aux1')

    def process_data(self, dates=None, files=None, seq=True, file_type='nc'):
        """ process the data to csv and aux formats
        input:
            dates = list of date
            file = list of file path match dates file index
        """
        if dates is None:
            #  get the file names and dates
            files, dates = get_file(self.output_path + f'/nc/*.{file_type}', freq='M')
            print(f'Processing {len(files)} files, estimated time: {len(files) * 0.12} hours')

        dfs = None  # make a list store a year of data
        if len(dates) == 0:  # no data found
            logger.info(f'No data found in {self.output_path + f"/nc/*.{file_type}"}')
            return -1
        elif len(dates) != 1:  # sort the dates
            months = pd.date_range(min(dates), max(dates), freq='MS')  # sort the dates
        else:
            months = dates  # only one date, no need to sort
            print(f'converting {months[0].strftime("%Y-%m")}')
            s2 = time.time()
            dfs = nc_to_df(None, files[dates.index(months[0])], False, 'xarray', False)
            print("break--------- \n")

            dfs.to_csv(os.path.join(self.output_path + '/csv1', f'{months[0].strftime("%Y%m")}.csv'))
            print('File took ' + str(((time.time()-s2)/60)) + 'm')
            return 0 
        processed_list = []  # list of processed files
        initial_date = months[0]

        '''-------------------------- Process .nc data for each month in sequential -----------------------------------'''
        if seq:  # process the data sequentially (limited memory)
            for i, m in tqdm(enumerate(months)):
                print(f'converting {m.strftime("%Y-%m")}')
                # try:
                print('Current file: ' + str(files[dates.index(m.date())]))     
                s1 = time.time()
                dfs = nc_to_df(None, files[dates.index(m.date())], False, 'xarray', False)
                # except:
                logger.info(f'No data for {m.strftime("%Y%m")}')
                print("break--------- \n")

                dfs.to_csv(os.path.join(self.output_path + '/csv1', f'{m.strftime("%Y%m")}.csv'))
                print('File took ' + str(((time.time()-s1)/60)) + 'm')
                dfs = None
                
            '''------------------- Process .nc  data for each month in parallel --------------------------------'''
            # 1 read each month in parallel --> convert to csv -->save as parquet
            # 2 read entire year in parallel --> convert to aux --> save aux

        else: 
            process_pool = []
            pool = mp.Pool(processes=int(mp.cpu_count() * 0.5), maxtasksperchild=100)  # create a pool of processes

            for m in months:
                try:
                    print(f"submitted {files[dates.index(m.date())]}")  # submit the process
                    process_pool.append(
                        [pool.apply_async(nc_to_df, args=(
                            None, files[dates.index(m.date())], False, 'xarray', True)), m])
                except:
                    logger.info(f'No data for {m.strftime("%Y%m")}')

            initial_date = process_pool[0][1]
            # TODO benchmark the performance of the code use dask or multiprocessing, or pandas
            for i, process in tqdm(enumerate(process_pool)):
                if dfs is None:
                    dfs = [process[0].get()]  # the return file is a path not a  df object
                else:
                    dfs.append(process[0].get())  # for large data avoid copy, use it dump it
                    print(f"complete {process[1].strftime('%Y%m')}")
                if (process[1].month == 12) | (i == (len(process_pool) - 1)) | (
                        (initial_date - process[1]) > datetime.timedelta(366)):
                    # save the processed data path to list for later use
                    processed_list.append([dfs, process[1].strftime("%Y")])
                    with open(self.output_path + '/processed_list.pickle', 'a+') as f:
                        yaml.dump({f'{process[1].strftime("%Y")}': dfs}, f)
                    dfs = None
                    initial_date = process[1]
            pool.close()
            pool.join()
            # now handover the processed data to dask, read the data in parallel
            for y in processed_list:
                df = dd.read_parquet(y[0])
                df = df.compute()
                # self.to_aux(df, y[1])
                print(f"saved {y[1]} to AUX")
            return 0

if __name__ == '__main__':
    main()
