##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 11JUL2017
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : worldweatheronline_api.py
# Brief Description      : This script will use World Weather Online's API to 
#                          gather weather elements for a given latitude/
#                          longitude at a particular date and hour.
# Data used              : 
# Output Files           : locations_weather_joined.csv
# Notes / Assumptions    : API Documentation: https://developer.worldweatheronline.com/api/
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 3.6.2
# Anaconda Version       : 5.0.1
# Spark Version          : n/a
# Operating System       : Windows 10
##############################################################################


import pandas as pd
import requests
import json
import datetime
import time

api_key = '<api_key_goes_here>'

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

startTime = time.time()

def get_historical_weather(df):
    
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        
        timestamp  = row['timestamp']
        latitude   = row['latitude']
        longitude  = row['longitude']
        date       = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
        hour       = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').strftime("%H")
        
        weather_url = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=' + str(api_key)  + '&q=' + str(latitude) + ',' + str(longitude) + '&format=json&extra=utcDateTime&date=' + date + '&includelocation=yes&tp=1'  
        weather_req = requests.get(weather_url)   
        if weather_req.status_code==200:
            jsondata = json.loads(weather_req.content)
        else:
            print('[ ERROR ] worldweatheronline.com Status Code: ' + str(weather_req.status_code))
        
        record = [hour_obs for hour_obs in jsondata['data']['weather'][0]['hourly'] if hour_obs['time'] == hour ]
        
        row['nearest_area']      = jsondata['data']['nearest_area'][0]['areaName'][0]['value']
        row['region']            = jsondata['data']['nearest_area'][0]['region'][0]['value']
        row['population']        = jsondata['data']['nearest_area'][0]['population']
        row['sunrise']           = jsondata['data']['weather'][0]['astronomy'][0]['sunrise']
        row['dailymaxtempC']     = jsondata['data']['weather'][0]['maxtempC'][0]
        row['dailymaxtempF']     = jsondata['data']['weather'][0]['maxtempF'][0]
        row['dailymintempC']     = jsondata['data']['weather'][0]['mintempC'][0]
        row['dailymintempF']     = jsondata['data']['weather'][0]['mintempF'][0]
        row['dailytotalSnow_cm'] = jsondata['data']['weather'][0]['totalSnow_cm'][0]
        row['dailysunHour']      = jsondata['data']['weather'][0]['sunHour'][0]
        row['dailyuvIndex']      = jsondata['data']['weather'][0]['uvIndex'][0]
        row['tempC']             = record[0]['tempC']
        row['tempF']             = record[0]['tempF']
        row['windspeedMiles']    = record[0]['windspeedMiles']
        row['windspeedKmph']     = record[0]['windspeedKmph']
        row['winddirDegree']     = record[0]['winddirDegree']
        row['winddir16Point']    = record[0]['winddir16Point']
        row['weatherCode']       = record[0]['weatherCode']
        row['weatherDesc']       = record[0]['weatherDesc'][0]['value']
        row['precipMM']          = record[0]['precipMM']
        row['humidity']          = record[0]['humidity']
        row['visibility']        = record[0]['visibility']
        row['pressure']          = record[0]['pressure']
        row['cloudcover']        = record[0]['cloudcover']
        row['HeatIndexC']        = record[0]['HeatIndexC']
        row['HeatIndexF']        = record[0]['HeatIndexF']
        row['DewPointC']         = record[0]['DewPointC']
        row['DewPointF']         = record[0]['DewPointF']
        row['WindChillC']        = record[0]['WindChillC']
        row['WindChillF']        = record[0]['WindChillF']
        row['WindGustMiles']     = record[0]['WindGustMiles']
        row['WindGustKmph']      = record[0]['WindGustKmph']
        row['FeelsLikeC']        = record[0]['FeelsLikeC']
        row['FeelsLikeF']        = record[0]['FeelsLikeF']
        
        df_out = df_out.append(row)
        
    return df_out

df2 = get_historical_weather(df)

endTime = time.time()

elapsedTime = endTime - startTime
print(elapsedTime)

df2.to_csv('locations_weather_joined.csv', index=False)
