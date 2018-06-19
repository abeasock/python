##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 19JUN2018
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 3.6.2
# Anaconda Version       : 5.0.1
# Spark Version          : n/a
# Operating System       : Windows 10
##############################################################################

# The countries.json file is avialable in my github.com/abeasock/datasets

# Python packages
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# Create a pandas DataFrame with latitudes, longitudes
coordinates = [('Rome', 41.9, 12.483333),
               ('Washington DC', 38.883333, -77),
               ('Moscow', 55.75, 37.6),
               ('Beijing', 39.91666667, 116.383333),
               ('Pretoria', -25.7, 28.216667)]
labels = ['capital_city', 'latitude', 'longitude']

capitals = pd.DataFrame(coordinates, columns=labels)

# Convert to Geopandas DataFrame
geom = capitals.apply(lambda x : Point([x['longitude'], x['latitude']]), axis=1)
crs = {'init' :'epsg:4326'}
capitals = gpd.GeoDataFrame(capitals, crs=crs, geometry=geom)

geojson_file = gpd.read_file('C:/Users/abeasock/Documents/Data/countries.json')

def join_capital_countries(capitals_file, geojson_file):
    """This function will join country metadata to capitals data if the 
       latitude/longitude of a captial falls within a country's polygon"""
    joined_df = gpd.sjoin(capitals_file, geojson_file, how='inner', op='within')
    joined_df.drop('geometry', axis=1, inplace=True)
    
    return joined_df.to_csv('C:/Users/abeasock/Documents/Data/capitals_countries_joined.txt', index=False, sep='|')

join_capital_countries(capitals, geojson_file)
