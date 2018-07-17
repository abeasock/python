##############################################################################
# Author                 : Amber Zaratisan
# Creation Date          : 17JUL2018
# Python Version         : 3.6.3
# Anaconda Version       : 5.0.1
# Operating System       : Windows 10
# Description			 : This script leverages the zipcodes package to 
#						   validate the city and states in the data by
#						   matching zip codes (only supports US zip codes)
##############################################################################

import pandas as pd
import zipcodes

data = [('27513', 'NC', 'Cary'),
        ('16066', 'PA', 'CRANBERRY TWP'),
        ('10005', 'NY', 'New York City'),
        ('89324', 'NC', 'NEW BERN')]

labels = ['zip_code', 'state', 'city']

df = pd.DataFrame(data, columns=labels)


def validate_zips(df):
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        
        record = zipcodes.matching(row['zip_code'])
        
        if len(record) > 0:
            row['validated_state'] = record[0]['state']
            row['validated_city'] = record[0]['city'] 
            if row['validated_state'] == row['state'].upper():
                row['state_match'] = 'Y'
            else:
                row['state_match'] = 'N'
            if row['validated_city'] == row['city'].upper():
                row['city_match'] = 'Y'
            else:
                row['city_match'] = 'N'
        else:
            row['validated_state'] = ''
            row['validated_city'] = ''
        
        df_out = df_out.append(row)
        
    return df_out

df2 = validate_zips(df)