import download
import merge
import read
import remove
import time as t
import os
from datetime import datetime, timedelta

#Authors: Brian Lee and Farnaz Safdarian

######################
# ! Required packages#
# * xarray           #
# * pandas           #
# * datetime         #
# * urllib           #
# * time             #
# * os               #
######################

#########################################################################################################################
# * This code downloads the NOMADS future weather data from the NCEP.gov.                                               #
# * 1 second delay had to be implemented to follow NCEP's predetermined download limit, 50 hits/minute = 1 download/sec.#
# ? Output = Data in pww file format from the present to 15 days later.                                                 #
# ! Do not change the time.sleep value in download.py, otherwise, you will get an IP ban!
#########################################################################################################################


#######################################
# * User Input: Insert the folder path#
#######################################
folder_path = "C:/Users/sl47745/Texas A&M University/Team - Overbye - Weather Droughts/CDSreading/csv_files/Future_weather/"

# * Create the Data folder for weather datas if the folder does not exist.
# * Remove function will delete all the datas in the folder once the code merges the whole data into one dataframe.
path = folder_path + "Data_csv"

exist = os.path.exists(path)

if not exist:
    os.makedirs(path)
    print("The new folder is created")

# * Automatically runs the code every 6 hours
while True:
    initial = 0
    final =  384
    time = datetime.now()
    hour = '{:%H}'.format(time)
    minute = '{:%M}'.format(time)
    # * NOMADS update the future weather data every 6 hours in terms of 12 AM, 06 AM, 12 PM, and 06 PM
    if (hour == '00' or hour == '06' or hour == '12' or hour == '18') and minute == '00':
        # * If the hour matches the condition, update the weather data
        day = '{:%d}'.format(time)
        month = '{:%m}'.format(time)
        year = '{:%Y}'.format(time)

        date = year + month + day # * The URL requires the date in form of 20240205
        # time = str(input(("Enter the starting time from select from [00, 06, 12, 18]: ")))
        time = hour
        print("Start Downloading data for " + date + time + "....")

        ##############
        # * Functions#
        ##############

        parameter = {
                1:"t2m", 2:"d2m", 3:"u10", 4:"v10", 5:"u100", 6:"v100", 7:"tcc"
            }
        # folder_path = "D:/Weather_Drought/ECMWF_open_data/"

        # download.download(initial,final,date,time,folder_path)
        # read.read(initial,final,date,time,parameter,folder_path)
        merge.merge(initial,final,date,time,parameter,folder_path)
        remove.remove(initial,final,date,time,parameter,folder_path) # ! Comment out this function if you do not want to remove the data from NCEP and csv files for each parameter 
        print("Finish downloading for " + date + time)
    
    # * Added 1 second dely every iteration to avoid s
    t.sleep(10)
