import os

def remove(x,y,z,t,parameter,folder_path):

    # * This function is to remove the all data that is no longer needed, such as csv files for each parameter or downloaded data
    # * Can be toggled off by commenting the remove function from main.py file
    
    while (x <= y):
        k = '{:03}'.format(x)
        grib = folder_path + "/Data_csv/" + z + t + "_" + k
        for j in range(1,8):
            csv = folder_path + "/Data_csv/" + parameter[j] + "_" +  k + ".csv"
            os.remove(csv)
        idx1 = folder_path + "/Data_csv/" + z + t + "_" + k + ".923a8.idx"
        idx2 = folder_path + "/Data_csv/" + z + t + "_" + k + ".9810b.idx"
        os.remove(grib)
        os.remove(idx1)
        os.remove(idx2)
        if (x < 120):
            x += 1
        else:
            x += 3