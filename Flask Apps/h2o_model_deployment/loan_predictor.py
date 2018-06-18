##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 16JAN2018
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : loan_predictor.py
# Bitbucket Repo         :
# Brief Description      : A basic Flask app to deploy a saved H2O model and
#                          predict if a person will default or not on a loan.
# Data used              : 
# Output Files           : 
#
# Notes / Assumptions    : The model was built in H2O (PySparkling) and saved 
#                          as a pickle file. The pickle file is loaded into 
#                          this script to score new data in the Flask app.
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 2.7
# Anaconda Version       : 5.0.0
# Spark Version          : 2.2.0
# Operating System       : Linux
#-----------------------------------------------------------------------------
#                           Change Control Information
#-----------------------------------------------------------------------------
# Programmer Name/Date   : Change and Reason
#
##############################################################################

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pysparkling import *
import os
import h2o
import random
from flask import Flask, render_template, request

################################################################################################
#
#   Index
#
################################################################################################

app = Flask(__name__)
app.secret_key = os.urandom(24)


@app.route('/', methods = ['GET','POST'])
@app.route('/index', methods = ['GET','POST'])
def index():
    
    if request.method == 'GET':
        return render_template('index.html')
    
    elif request.method == 'POST':
        loan_amnt       = float(request.form.get('loan_amnt', 5000))
        term            = request.form.get('term', '36 months')
        emp_length      = request.form.get('emp_length', '1 year')
        home_ownership  = request.form.get('home_ownership', 'RENT')
        purpose         = request.form.get('purpose', 'credit card')
        addr_state      = request.form.get('addr_state', 'AL')
        annual_inc      = float(request.form.get('annual_inc',  0))
        inq_last_6mths  = float(request.form.get('inq_last_6mths', 0))

        col_names       = ['loan_amnt', 'term', 'emp_length', 'home_ownership', 'purpose', 
                           'addr_state', 'annual_inc', 'inq_last_6mths']

        df              = h2o.H2OFrame.from_python([(loan_amnt, term, emp_length, home_ownership, 
                                                     purpose, addr_state, annual_inc, inq_last_6mths)], 
                                                    column_names=col_names)
        # Convert string variables into factors
        string_vars = [i[0] for i in df.types.iteritems() if i[1]=='string']
        for var in string_vars:
            df[var] = df[var].asfactor()
        
        saved_model     = h2o.load_model('/assets/flask_deployment_demo/GBM_model_python_1515678740025_16')
        predicted       = saved_model.predict(df)
        predicted_df    = predicted.as_data_frame()
        default_prob    = round(predicted_df['default'][0], 3)

        # Append "predicted scores" to original DF.
        df_predictions = df.cbind(predicted)
        
        if default_prob < 0.5:
            result = 'Likely to not default'
        else:
            result = 'Likely to default'
        
        return render_template('index.html', result=result, default_prob=default_prob, \
                loan_amnt=loan_amnt, term=term, emp_length=emp_length, home_ownership=home_ownership, \
                purpose=purpose, addr_state=addr_state, annual_inc=annual_inc, inq_last_6mths=inq_last_6mths)


################################################################################################
#
#   App
#
################################################################################################

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    hc = H2OContext.getOrCreate(spark)
    app.run(debug=False, threaded=True, host='0.0.0.0', port=5555)
