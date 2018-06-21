##############################################################################
# Author                 : Amber Zaratisan
# Creation Date          : 21Jun2018
# Python Version         : 3.5.4
# Anaconda Version       : 5.0.1
# Operating System       : Windows 10
##############################################################################

import pandas as pd
import pandasql as pdsql

# Dummy datasets
tips = pd.DataFrame(list(zip([1, 24, 33, 50, 82], 
							 [7, 5, 3, 6, 2], 
							 [97.89, 76.14, 48.02, 84.23, 24.19], 
							 [97.89, 76.14, 48.02, 84.23, 24.19])), 
				    columns=['ID', 'size', 'total_bill', 'amount'])

tax = pd.DataFrame(list(zip([1, 24, 33, 50, 82], 
						    ['ARG', 'US', 'ARG', 'MEX', 'ARG'], 
						    [1.3, 5.2, 2.4, 1.8, 3.5])), 
				   columns=['ID', 'country', 'fx_rate'])


# Write a SQL Query as a string and save it to a variable
sql_query = """
	        SELECT sum(a.amount*b.fx_rate)
	        FROM tips a
	        INNER JOIN tax b
	        ON a.id=b.id
	        WHERE (a.size >= 5 OR a.total_bill > 45)
	        And b.country='ARG';
	        """

# Query Pandas DataFrame with SQL
df = pdsql.sqldf(sql_query, locals())

# Print new dataset resulting from SQL query
df.head()