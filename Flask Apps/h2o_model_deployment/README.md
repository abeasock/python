# Loan Predictor Flask App

This is a simple Flask app to show real time model scoring. The app accepts a POST request, which a saved model scores and the model output is returned.

Please see my repo open_source_demo to obtain the data used to build the model as well as the Zeppelin Notebook with the data prep and modeling technique. This app folder only contains the saved model (an H2O GBM), which is loaded into the loan_predictor.py script to score new data.
https://github.com/abeasock/open_source_demo
