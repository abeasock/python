##############################################################################
# Author            : Amber Zaratsian
# Creation Date     : 17AUG2018
# Description       : Function that takes a PySpark DataFrame and a list of
#					 categorical colmnns and returns the DataFrame with 
#					  additional dummy encoded columns for the categorical
#					  variables. Like the pandas.get_dummies() in Python.
# Python Version    : 3.5.4
# Anaconda Version  : 5.0.1
# Spark Version		: 2.1.0
# Operating System  : Linux
##############################################################################


from pyspark.sql import functions as F
import re

# Create dummy data with categorical strings
pets = [('female', 'dog'), ('male', 'dog'), ('male', 'cat'), ('female', 'dog'), ('male', 'cat')]
pets_df = spark.createDataFrame(pets, ['gender', 'species'])

pets_df.show()
# Original DateFrame
#+------+-------+
#|gender|species|
#+------+-------+
#|female|    dog|
#|  male|    dog|
#|  male|    cat|
#|female|    dog|
#|  male|    cat|
#+------+-------+

# Convert categorical variables in dummy variables
def create_dummies(df,dummylist):
    for inputcol in dummylist:
        categories = df.select(F.lower(F.col(inputcol))).rdd.distinct().flatMap(lambda x: x).collect()
        exprs = [(re.sub('[^a-zA-Z0-9]','_',category), F.when(F.lower(F.col(inputcol)) == category, 1).otherwise(0).alias(category)) for category in categories]
        for index,expr in enumerate(exprs):
           df=df.withColumn(inputcol+"_"+str(expr[0]), expr[1])
    return df

categoricalColumns = ['gender', 'species']

pets_df_2 = create_dummies(pets_df, categoricalColumns)

pets_df_2.show()

# Tranformed DataFrame
#+------+-------+-------------+-----------+-----------+-----------+
#|gender|species|gender_female|gender_male|species_dog|species_cat|
#+------+-------+-------------+-----------+-----------+-----------+
#|female|    dog|            1|          0|          1|          0|
#|  male|    dog|            0|          1|          1|          0|
#|  male|    cat|            0|          1|          0|          1|
#|female|    dog|            1|          0|          1|          0|
#|  male|    cat|            0|          1|          0|          1|
#+------+-------+-------------+-----------+-----------+-----------+
