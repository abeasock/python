##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 06JUN2018
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : shp2json.py
# Bitbucket Repo         :
# Brief Description      : This script will convert all shapefiles to geojson
#                          in the provided directory.
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

import os
import shapefile
from datetime import datetime, date
from json import dumps

def shp2json(in_path, out_path):  
  for filename in os.listdir(in_path):
    if filename.endswith(".shp"):
      # read the shapefile
      reader = shapefile.Reader(in_path+filename)
      fields = reader.fields[1:]
      field_names = [field[0] for field in fields]
      buffer = []
      for sr in reader.shapeRecords():
          atr = dict(zip(field_names, sr.record))
          geom = sr.shape.__geo_interface__
          buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 
      def json_serial(obj):
        if isinstance(obj, (datetime, date)):
          return obj.isoformat()
        else:
          return obj
      # write the GeoJSON file
      os.makedirs(os.path.dirname(out_path), exist_ok=True)
      geojson = open(out_path + filename[:-4] + ".json", "w")
      geojson.write(dumps({"type": "FeatureCollection","features": buffer}, indent=2, default=json_serial) + "\n")
      geojson.close()

# Modify input_path for location of shapefiles and out_path for where the geojson files should be written to
#shp2json(in_path=<>, out_path=<>)


# Iterate through directory and apply the shp2json function to all subdirectories. All folders must first be unzipped.
dir_name = <update with path>

for dirs, subdir, files in os.walk(dir_name):
  for sd in subdir:
    print(os.path.join(dirs, sd))
    shp2json(in_path=os.path.join(dirs, sd) + "/", out_path= os.path.join(dirs, sd)+"_geojson/")
