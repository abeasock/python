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
# Script                 : weatherunderground_api.py
# Brief Description      : This script will use Weather Underground's API to 
#                          do a geolookup and gather weather elements for a 
#                          given latitude/longitude at a particular date and 
#                          hour.
# Data used              : 
# Output Files           : locations_weather_joined.csv
#
# Notes / Assumptions    : WU Documentation: https://www.wunderground.com/weather/api/d/docs?d=index
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
import datetime

api_key = <your api key>

# Create a pandas DataFrame with latitudes, longitudes
locations = [('2/14/2014 12:31', 39.633556, -86.813806),
             ('5/19/2016 23:01', 41.992934, -86.128012),
             ('12/29/2017 20:05', 39.975028, -81.577583),
             ('4/7/2016 3:00', 44.843667, -87.421556),
             ('7/2/2015 5:45', 39.794824, -76.647191)]
labels = ['date', 'latitude', 'longitude']

df = pd.DataFrame(locations, columns=labels)

# Convert string to datetime
df['timestamp'] = df['date'].map(lambda x: datetime.datetime.strptime(x,'%m/%d/%Y %H:%M'))
df.drop('date', axis=1, inplace=True)


def get_historical_weather(df):
    
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        
        timestamp  = row['timestamp']
        latitude   = row['latitude']
        longitude  = row['longitude']
        date       = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
        hour       = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').strftime("%H")
        
        weather_url = 'http://api.wunderground.com/api/' + api_key + '/geolookup/history_' + str(date) + '/q/' + str(latitude) + ',' + str(longitude) + '.json'   
        weather_req = requests.get(weather_url)   
        if weather_req.status_code==200:
            jsondata = json.loads(weather_req.content)
        else:
            print('[ ERROR ] WUnderground.com Status Code: ' + str(weather_req.status_code))
        
        record = [hour_obs for hour_obs in jsondata['history']['observations'] if hour_obs['utcdate']['hour'] == hour ]
        
        row['heatindexm']  = record[0]['heatindexm']
        row['windchillm']  = record[0]['windchillm']
        row['wdire']       = record[0]['wdire']
        row['wdird']       = record[0]['wdird']
        row['windchilli']  = record[0]['windchilli']
        row['hail']        = record[0]['hail']
        row['heatindexi']  = record[0]['heatindexi']
        row['precipi']     = record[0]['precipi']
        row['thunder']     = record[0]['thunder']
        row['pressurei']   = record[0]['pressurei']
        row['snow']        = record[0]['snow']
        row['pressurem']   = record[0]['pressurem']
        row['fog']         = record[0]['fog']
        row['icon']        = record[0]['icon']
        row['precipm']     = record[0]['precipm']
        row['conds']       = record[0]['conds']
        row['tornado']     = record[0]['tornado']
        row['hum']         = record[0]['hum']
        row['tempi']       = record[0]['tempi']
        row['tempm']       = record[0]['tempm']
        row['dewptm']      = record[0]['dewptm']
        row['rain']        = record[0]['rain']
        row['dewpti']      = record[0]['dewpti']
        row['visi']        = record[0]['visi']
        row['vism']        = record[0]['vism']
        row['wgusti']      = record[0]['wgusti']
        row['metar']       = record[0]['metar']
        row['wgustm']      = record[0]['wgustm']
        row['wspdi']       = record[0]['wspdi']
        row['wspdm']       = record[0]['wspdm']
        
        df_out = df_out.append(row)
        
    return df_out

df2 = get_historical_weather(df)

df2.to_csv('locations_weather_joined.csv', index=False)
