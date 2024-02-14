import pandas as pd
from datetime import datetime, timedelta
import struct
import pytz 
import os
import numpy as np
import xarray as xr

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

def format_string(dateString):
            dateObj = datetime.strptime(dateString, '%m/%d/%Y %H:%M')
            returnStr = dateObj.strftime('%Y-%m-%d') + 'T' + dateObj.strftime('%H') + ':00:00.000Z'
            return returnStr

def merge(x,y,z,t,parameter,folder_path):
    ################################
    # * Automate the merge process #
    ################################
    x_initial = x
    # count = 0
    df2 = pd.DataFrame()

    time = datetime.now()
    day = '{:%d}'.format(time)
    month = '{:%m}'.format(time)
    year = '{:%Y}'.format(time)
    while (x <= y):

        # * read all the data and merge into one dataframe
        for j in range(1,8): 
            k = '{:03}'.format(x)
            path = folder_path +"/Data_csv/" + str(parameter[j]) + "_" + k + ".csv"
            if j == 1:
                df = pd.read_csv(path)
            else:
                df1 = pd.read_csv(path)
                df[parameter[j]] = df1[parameter[j]]

        # if x == x_initial or x % 24 == 0:
        if x == x_initial:
            df2 = df
            # x_initial = x
        else:
            df2 = pd.concat([df2,df], ignore_index= True)

        ##########################################################
        # * Save the datafram in csv file format by daily.       #
        # * For every data past 5 days will contain 0 am to 11 pm#
        # * For every data after 5 days will contain 0am to 9 pm #
        ##########################################################

        if (x < 120):

            # if count % 24 == 23:
            #     time = datetime.now() + timedelta(hours=x_initial)
            #     day = '{:%d}'.format(time)
            #     month = '{:%m}'.format(time)
            #     year = '{:%Y}'.format(time)
            #     df2.to_csv('NorthAmerica' + year+ '_' + month + '_' + day+'.csv',index=False)
            #     df2 = pd.DataFrame()
            # count += 1

            x += 1
 
        else:

            # if count % 24 ==  21:
            #     time = datetime.now() + timedelta(hours=x_initial)
            #     day = '{:%d}'.format(time)
            #     month = '{:%m}'.format(time)
            #     year = '{:%Y}'.format(time)
            #     df2.to_csv('NorthAmerica' + year+ '_' + month + '_' + day+'.csv',index=False)
            #     df2 = pd.DataFrame()
            # count += 3

            x += 3

    # df2.to_csv('Forecast_NorthAmerica_Run' + year + '-' + month + '-' + day + 'T' + t + 'Z' + '.csv',index=False)


    df_station=pd.read_csv(folder_path + "station.csv")
    df_station['Region']=df_station['Region'].fillna('')
    df_station['Country2']=df_station['Country2'].fillna('')
    df_station['ElevationMeters']=df_station['ElevationMeters'].astype(int)
    df_station['Region']=df_station['Region'].astype(str)
    df_station['Country2']=df_station['Country2'].astype(str)
    df_station.reset_index(inplace=True)
    df_station['WhoAmI'] =   '+' + df_station['Latitude'].apply(to_str, args=(5,))+df_station['Longitude'].apply(to_str, args=(6,))+'/'
    df_station['WhoAmI'].drop_duplicates(inplace=True)
    df_station = df_station.sort_values(by=['WhoAmI'])
    LOC=df_station['WhoAmI'].nunique()

    df= df2

    df['longitude']=df['longitude']-360
    df['WhoAmI'] =   '+' + df['latitude'].apply(to_str, args=(5,))+df['longitude'].apply(to_str, args=(6,))+'/'
    df['sped'] = np.sqrt(df['u10'] ** 2 + df['v10'] ** 2)
    df['sped100'] = np.sqrt(df['u100'] ** 2 + df['v100'] ** 2)
    df['drct'] = np.arctan2(df['u10'], df['v10'])
    # unit conversion
    df['t2m'] = np.round((df['t2m'] - 273.15) * 9 / 5 + 32)  # convert to degF
    df['d2m'] = np.round((df['d2m'] - 273.15) * 9 / 5 + 32)  # convert to degF
    df['sped'] = np.round(df['sped'] * 2.23694)  #convert from mps to mph
    #df['tcc'] = np.round(df['tcc'] * 100)  # convert to %
    df['drct'] = np.round(df['drct'] * 180 / np.pi + 180)  # convert to deg
    df['DirectHorizontalIrradianceWM2']=255*5
    df['GlobalHorizontalIrradianceWM2']=255*5
    df['WindSpeed100mph'] = df['sped100'] * 2.23694  #convert from mps to mph

    df.drop(columns=['longitude', 'latitude', 'u100', 'sped100', 'v100', 'u10', 'v10'], inplace=True)
    df.rename( 
            columns={"valid_time": "UTCISO8601", "WhoAmI": "WhoAmI", 't2m': 'tempF', 'd2m': 'DewPointF',
                    "sped": "WindSpeedmph",
                    "sped100": "WindSpeed100mph",
                    "drct": "WindDirection",
                    'tcc': 'CloudCoverPerc'}, inplace=True)
    df_new_columnlist = ['UTCISO8601','WhoAmI','DewPointF','tempF', 'GlobalHorizontalIrradianceWM2','CloudCoverPerc','DirectHorizontalIrradianceWM2','WindSpeedmph','WindDirection','WindSpeed100mph']
    df = df.reindex(columns = df_new_columnlist)
    df = df.sort_values(by=['UTCISO8601'])

    df['UTCISO8601']= pd.to_datetime(df['UTCISO8601'],format='ISO8601').dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    try:
        df = df.astype(
                    {'WhoAmI': 'str', 'tempF': 'int16', 'DewPointF': 'int16', 'WindSpeedmph': 'int8', 'WindDirection': 'int16', 'dswrf': 'int8', 
                    'CloudCoverPerc': 'int16', 'WindSpeed100mph':'int16'})
    except:
        df = df.astype(
                    {'WhoAmI': 'str', 'tempF': 'int16', 'DewPointF': 'int16', 'WindSpeedmph': 'int8', 'WindDirection': 'int16', 'GlobalHorizontalIrradianceWM2': 'int16', 
                    'DirectHorizontalIrradianceWM2': 'int16', 'CloudCoverPerc': 'int16', 'WindSpeed100mph':'int16'})
        
    # Define the conversion function # Excel's epoch starts on "January 0, 1900"
    def datetime_to_excel_double(date_obj):
        excel_epoch = datetime(1899, 12, 31, tzinfo=pytz.UTC)  # Make it timezone-aware by setting the timezone to UTC
        delta = date_obj - excel_epoch
        excel_date = delta.days + (delta.seconds / 86400.0) + (date_obj.microsecond / 86400e6)+1
        return excel_date

    # Convert UTCISO8601 to timestamp
    df['UTCISO8601'] = pd.to_datetime(df['UTCISO8601'])
    df['excel_double_datetime'] = df['UTCISO8601'].apply(datetime_to_excel_double)
    df = df.sort_values(by=['excel_double_datetime', 'WhoAmI'])
    #df['excel_double_datetime'] = df['excel_double_datetime'].astype(float)
    COUNT=df['excel_double_datetime'].nunique()
    unique_dates = df['excel_double_datetime'].unique()
    df['tempF102'] = df['tempF'] + 115
    df['DewPointF104'] = df['DewPointF'] + 115
    df['WindDirection107'] = df['WindDirection'] /5
    df['GlobalHorizontalIrradianceWM2_120'] = df['GlobalHorizontalIrradianceWM2'] /5
    df['DirectHorizontalIrradianceWM2_121'] = df['DirectHorizontalIrradianceWM2'] /5
    df['WindDirection107'] = df['WindDirection107'].astype(int)
    df['GlobalHorizontalIrradianceWM2_120'] = df['GlobalHorizontalIrradianceWM2_120'].astype(int)
    df['DirectHorizontalIrradianceWM2_121'] = df['DirectHorizontalIrradianceWM2_121'].astype(int)

    # Extract filename without extension
    file_name = 'Forecast_NorthAmerica_Run' + year + '-' + month + '-' + day + 'T' + t + 'Z' 
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
    aPWWFileName = folder_path + file_name + ".pww"  

    # Extracting the smallest start time and the largest end time from the DataFrame and converting them to timestamps
    aStartDateTimeUTC = df['excel_double_datetime'].min()
    aEndDateTimeUTC = df['excel_double_datetime'].max()
    #area=[58, -130, 24, -60] North 58°, West -130°, South 24°, East -60°  
    aMinLat=int(df_station['Latitude'].min()) 
    aMaxLat= int(df_station['Latitude'].max()) 
    aMinLon=int(df_station['Longitude'].min())
    aMaxLon=int(df_station['Longitude'].max())
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
        file.write(struct.pack('<i', 0))
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
        for date in unique_dates:
            file.write(struct.pack('<d', date))
            
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
            rows = df[df['excel_double_datetime'] == date]
            
            
            
            
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



