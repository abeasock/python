##############################################################################
# Author                 : Amber Zaratisan
# Creation Date          : 02FEB2018
# Python Version         : 3.5.4
# Anaconda Version       : 5.0.1
# Operating System       : Windows 10
##############################################################################

import os, zipfile

dir_name = 'C:/Users/abeasock/Documents/'
extension = ".zip"

"""
Description: This will unzip all zipped files in a given directory to that 
             directory & REMOVE the zipped file

Before: root/directory/folder.zip 
After : root/directory/folder
"""

for dirs, subdir, files in os.walk(dir_name):
    for file in files:
        if file.endswith(".zip"): # check for ".zip" extension
            filename = os.path.join(dirs, file) # get full path of files
            print(filename)
            zip_ref = zipfile.ZipFile(filename) # create zipfile object
            zip_ref.extractall(dirs) # extract file to dir
            zip_ref.close() # close file
            os.remove(filename) # remove zipped file


"""
Description:  This will unzip all zipped files in a given directory to the same 
              level as the directory & REMOVE the zipped file

Before: root/directory/folder.zip 
After : root/directory
        root/folder
"""

for dirs, subdir, files in os.walk(dir_name):
    for file in files:
        if file.endswith(".zip"): # check for ".zip" extension
            filename = os.path.join(dirs, file) # get full path of files
            print(file)
            directory = dir_name + file[4:10] # want to name new folder a substring of the zipped file name
            try:
                os.stat(directory) # see if directory exists
            except:
                os.mkdir(directory) # make directory if does not exist
            zip_ref = zipfile.ZipFile(filename) # create zipfile object
            zip_ref.extractall(directory) # extract file to dir
            zip_ref.close() # close file
            os.remove(filename) # remove zipped file

