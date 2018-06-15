##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 06SEP2017
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : openstreetmaps.py
# Brief Description      : This script is intended to collect road information
#                          from OpenStreetMaps for a given DataFrame with
#                          latitude/longitude for each record.
# Data used              : 
# Output Files           : 
#
# Notes / Assumptions    : 
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 3.6.2
# Anaconda Version       : 5.0.1
# Spark Version          : n/a
# Operating System       : Windows 10
#-----------------------------------------------------------------------------
#                           Change Control Information
#-----------------------------------------------------------------------------
# Programmer Name/Date   : Change and Reason
#
##############################################################################

import pandas as pd
import requests
import json

# Create a pandas DataFrame with latitudes, longitudes
locations = [('2/14/2014 12:31', 39.633556, -86.813806),
             ('5/19/2016 23:01', 41.992934, -86.128012),
             ('12/29/2017 20:05', 39.975028, -81.577583),
             ('4/7/2016 3:00', 44.843667, -87.421556),
             ('7/2/2015 5:45', 39.794824, -76.647191)]
labels = ['timestamp', 'latitude', 'longitude']
df = pd.DataFrame(locations, columns=labels)


def get_road_metadata(df):
    """Extracting the osm_id and address from OpenStreetMaps for a given latitude/longitude"""
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        latitude   = row['latitude']
        longitude  = row['longitude']
        
        url = "http://nominatim.openstreetmap.org/reverse?format=json&lat=" + str(latitude) + "&lon=" + str(longitude) + "&zoom=18&addressdetails=1"
        r = requests.get(url) 
        
        if r.status_code==200:
            jsondata = json.loads(r.content)
        else:
            print('[ ERROR ] openstreetmap.com Status Code: ' + str(r.status_code))  
        
        row['osm_id']       = jsondata['osm_type'][0].upper() + jsondata['osm_id']
        row['display_name'] = jsondata['display_name']
        
        df_out = df_out.append(row)
    return df_out

df2 = get_road_metadata(df)    


def get_road_metadata(df):
     """Extracting road information and address elements from OpenStreetMaps for a given osm_id"""
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        osm_id  = row['osm_id']
        
        url = 'http://nominatim.openstreetmap.org/lookup?format=json&osm_ids=' + osm_id
        r = requests.get(url) 
        
        if r.status_code==200:
            jsondata = json.loads(r.content)
        else:
            print('[ ERROR ] openstreetmap.com Status Code: ' + str(r.status_code))
            
        row['road_class']   = jsondata[0]['class']
        row['road_type']    = jsondata[0]['type']
        row['address']      = jsondata[0]['address']['road']
        row['county']       = jsondata[0]['address']['county']
        row['state']        = jsondata[0]['address']['state']
        row['zip']          = jsondata[0]['address']['postcode']
        row['country']      = jsondata[0]['address']['country']
        
        df_out = df_out.append(row)
    return df_out          

df3 = get_road_metadata(df2)
