import xarray as xr
import pandas as pd

def read(x,y,z,t,parameter,folder_path):
    while (x <= y):
        
        # * Copy and paste the folder path where you downloaded the data
        k = '{:03}'.format(x)
        path = folder_path + "/Data_csv/" + z + t + "_" + k

        for j in range(1,8):
            # * Reading grib2 file
            ####################################################################
            # * parameter list                                                 #
            # * 1:"t2m", 2:"d2m", 3:"u10", 4:"v10", 5:"u100", 6:"v100", 7:"tcc"#
            ####################################################################
            if j == 1 or j == 2:
                dataset = xr.open_dataset(path, engine='cfgrib',backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}}) # * Reading 2m Temperature and Dew point
            elif j == 3 or j == 4:
                dataset = xr.open_dataset(path, engine='cfgrib',backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 10}}) # * Reading 10m UGRD and VGRD
            elif j == 5 or j == 6:
                dataset = xr.open_dataset(path, engine='cfgrib',backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 100}}) # * Reading 100m UGRD and VGRD
            else:
                dataset =  xr.open_dataset(path,
                                    engine='cfgrib',
                                    backend_kwargs={'filter_by_keys': {'stepType': 'instant', 'typeOfLevel': 'atmosphere'}}) # * Reading Cloud coverage in entire atmosphere level as instant data

            # * Convert the grib2 file into dataframe and make it into csv format file
            df = dataset[parameter[j]].to_dataframe()
            df.to_csv(folder_path + "/Data_csv/" + parameter[j] + "_" +  k + ".csv")

        if (x < 120):
            x += 1
        else:
            x += 3


