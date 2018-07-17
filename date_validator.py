##############################################################################
# Author                 : Amber Zaratisan
# Creation Date          : 17JUL2018
# Python Version         : 3.6.3
# Anaconda Version       : 5.0.1
# Operating System       : Windows 10
# Description            : This script takes a column of dates (in string 
#                          format) and creates a column flagging if the date
#                          is valid (1) or not (0) or if the date is missing 
#                          the flag will be left blank.
##############################################################################

import pandas
from datetime import datetime

data = [('2/14/2014'),
        ('5/19/2016'),
        (''),
        ('13/7/2016'),
        ('7/41/2015')]
labels = ['date']

df = pd.DataFrame(data, columns=labels)

str_format = '%m/%d/%Y'

def validate_dates(df):
    df_out = pd.DataFrame()
    
    for index, row in df.iterrows():
        if row['date'] != '':  # if date is not missing
            try:
                datetime.strptime(row['date'], str_format)
                row['valid_date'] = 1
            except:
                row['valid_date'] = 0
        else:
            row['valid_date'] = ''
        df_out = df_out.append(row)
        
    return df_out

df2 = validate_dates(df)
