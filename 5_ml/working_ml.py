import dask.dataframe as dd

from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import dask.array as da

from dask_ml.preprocessing import Categorizer, DummyEncoder, OrdinalEncoder
from dask_ml.model_selection import train_test_split
from dask_ml.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import make_pipeline

from sklearn.metrics import accuracy_score, mean_squared_error
from xgboost import dask as dxgb
import numpy as np

import time
import os
import sys

base_dir_nk = '/d/hpc/projects/FRI/bigdata/students/nk93594/'
base_dir_cb = '/d/hpc/projects/FRI/bigdata/students/cb17769/'

cleaned_dataset_dir = base_dir_cb + 'cleaned_data.parquet/'
weather_dataset_dir = base_dir_cb + 'augmented_data/weather.parquet/'
events_dataset_dir = base_dir_cb + 'augmented_data/events.parquet/'
hs_dataset_dir = base_dir_cb + 'augmented_data/hs.parquet/'

# Function for Dask-ML Logistic Regression
def dask_ml_logistic_regression(X_train, y_train, X_test, y_test):
    log_reg = LogisticRegression()

    # Training
    start_time = time.time()
    log_reg.fit(X_train, y_train)
    train_time = time.time() - start_time
    
    # Prediction
    start_time = time.time()
    y_pred = log_reg.predict(X_test)
    prediction_time = time.time() - start_time
    
    # Evaluation
    metric = accuracy_score(y_test.compute(), y_pred.compute())
    
    return metric, train_time, prediction_time

# Function for XGBoost
def dask_xgboost(client, X_train, y_train, X_test, y_test):
    
    num_class = len(da.unique(y_train).compute())

    params = {
        'objective': 'multi:softmax' if num_class > 2 else 'binary:logistic',
        'num_class': num_class,
        'learning_rate': 0.1,
        'max_depth': 6,
        #'n_estimators': 100
    }
    
    dtrain = dxgb.DaskDMatrix(client, X_train, y_train)
    dtest = dxgb.DaskDMatrix(client, X_test, y_test)

    # Training
    start_time = time.time()
    results = dxgb.train(client, params, dtrain, num_boost_round=100)
    train_time = time.time() - start_time

    bst = results['booster']
    
    # Prediction
    start_time = time.time()
    y_pred = dxgb.predict(client, bst, dtest)
    prediction_time = time.time() - start_time

    # Evaluation
    if num_class == 2:
        metric = accuracy_score(y_test.compute(), y_pred > 0.5)
    else:
        metric = accuracy_score(y_test.compute(), y_pred)
    
    return metric, train_time, prediction_time

# Function for Scikit-Learn SGD Classifier with partial_fit
def sklearn_sgd_partial_fit(X_train, y_train, X_test, y_test):
    classes = y.unique().compute().astype('int')
        
    sgd = SGDClassifier(max_iter=1000, tol=1e-3)
        
    # Training
    start_time = time.time()
    for i in range(0, len(X_train), 1000):
        X_batch = X_train[i:i+1000].compute()
        y_batch = y_train[i:i+1000].compute()
        sgd.partial_fit(X_batch, y_batch, classes=classes)
    train_time = time.time() - start_time
    
    # Prediction
    start_time = time.time()
    y_pred = sgd.predict(X_test.compute())
    prediction_time = time.time() - start_time
        
    # Evaluation
    metric = accuracy_score(y_test.compute(), y_pred)

    return metric, train_time, prediction_time


if __name__ == '__main__':
    
    prob = sys.argv[1]
    
    n_workers = 6
    memory_limit = 8
    cluster = SLURMCluster(cores=5, # Number of cores per job
                           processes=1, 
                           memory=f"{memory_limit}GB", # Memory per job
                           job_extra_directives=['--time=06:00:00']
                        )
    client = Client(cluster)
    cluster.scale(n=n_workers) # Ask for 5 workers
    
    df = dd.read_parquet(cleaned_dataset_dir, assume_missing = True)
    
    # High Ticket days
    if prob == '1':
        print('Solving problem 1: Predicting high ticket days')

        # Aggregating tickets by day
        df['issue_date'] = dd.to_datetime(df['issue_date'])
        df['_date'] = df['issue_date'].dt.date
        daily_tickets = df.groupby('_date').size().reset_index()
        daily_tickets.columns = ['issue_date', 'ticket_count']
        del df

        # Creating a binary target for high number of tickets (e.g., above median)
        median_ticket_count = daily_tickets['ticket_count'].median().compute()
        print('Median ticket count:', median_ticket_count)
        daily_tickets['high_ticket_day'] = daily_tickets['ticket_count'] > median_ticket_count
        
        # Preprocessing
        daily_tickets['issue_date'] = dd.to_datetime(daily_tickets['issue_date'])
        daily_tickets['day_of_week'] = daily_tickets['issue_date'].dt.dayofweek
        daily_tickets['month'] = daily_tickets['issue_date'].dt.month

        # Splitting the data into features and target
        X = daily_tickets[['day_of_week', 'month']]
        y = daily_tickets['high_ticket_day']
        del daily_tickets

    # Violation code
    elif prob == '2':
        print('Solving problem 2: Predicting violation code')

        # Preprocess data
        
        features = ['violation_county', 'violation_precinct', 'street_code', 'issue_date', 'violation_code', 'vehicle_make']

        df = df[features]
        df = df.dropna(subset=features)
        df = df.drop_duplicates()
        df['violation_code'] = df['violation_code'].astype(int)

        # Extract features from issue_date
        df['issue_date'] = dd.to_datetime(df['issue_date'])
        df['day_of_week'] = df['issue_date'].dt.dayofweek
        df['hour'] = df['issue_date'].dt.hour
        df['time_of_day'] = df['hour'].apply(lambda x: 'morning' if 6 <= x < 12 
                                            else 'afternoon' if 12 <= x < 18 
                                            else 'evening' if 18 <= x < 24 
                                            else 'night', meta=('x', 'str'))

        # Prepare features and target
        # DELETE street name because it has too many unique values
        X = df[['violation_county', 'violation_precinct', 'day_of_week', 'time_of_day', 'vehicle_make']].persist()
        y = df['violation_code'].persist()
        del df

        print(X.head())
        print(y.head())

        vehicle_make = X['vehicle_make'].value_counts().compute()
        print(vehicle_make)
        print('Length of vehicle make:', len(vehicle_make))
        # Vehicle make has too many unique values, impossible to encode

        # Categories and encode features
        categorizer = Categorizer(columns=['violation_county', 'violation_precinct', 'time_of_day', 'vehicle_make'])
        X = categorizer.fit_transform(X)

        ordinal_encoder = OrdinalEncoder(columns=['time_of_day'])
        X = ordinal_encoder.fit_transform(X)

        dummy_encoder = DummyEncoder(columns=['violation_county', 'violation_precinct', 'vehicle_make'], drop_first=True)
        X = dummy_encoder.fit_transform(X)


    ## Common training and evaluation steps

    # split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)

    # Convert to Dask Array for XGBoost
    multiplier = 4
    X_train_da = X_train.repartition(npartitions=n_workers * multiplier).to_dask_array(lengths=True)
    y_train_da = y_train.repartition(npartitions=n_workers * multiplier).to_dask_array(lengths=True)
    X_test_da = X_test.repartition(npartitions=n_workers * multiplier).to_dask_array(lengths=True)
    y_test_da = y_test.repartition(npartitions=n_workers * multiplier).to_dask_array(lengths=True)
    
    # Running and comparing all models
    #log_reg_accuracy, log_reg_train_time, log_reg_pred_time = dask_ml_logistic_regression(X_train_da, y_train_da, X_test_da, y_test_da)
    #print(f"Dask-ML Logistic Regression - Accuracy: {log_reg_accuracy:.4f}, Train Time: {log_reg_train_time:.2f} sec, Prediction Time: {log_reg_pred_time:.2f} sec")

    xgb_accuracy, xgb_train_time, xgb_pred_time = dask_xgboost(client, X_train_da, y_train_da, X_test_da, y_test_da)
    print(f"XGBoost - Accuracy: {xgb_accuracy:.4f}, Train Time: {xgb_train_time:.2f} sec, Prediction Time: {xgb_pred_time:.2f} sec")

    #sgd_accuracy, sgd_train_time, sgd_pred_time = sklearn_sgd_partial_fit(X_train_da, y_train_da, X_test_da, y_test_da)
    #print(f"SGD Classifier - Accuracy: {sgd_accuracy:.4f}, Train Time: {sgd_train_time:.2f} sec, Prediction Time: {sgd_pred_time:.2f} sec")
    
    client.close()
    cluster.close()


## RESULTS

# Solving problem 1: Predicting high ticket days
# Median ticket count: 8550.0
# Dask-ML Logistic Regression - Accuracy: 0.5206, Train Time: 7977.60 sec, Prediction Time: 0.01 sec
# XGBoost - Accuracy: 0.5437, Train Time: 6.92 sec, Prediction Time: 12.62 sec
# SGD Classifier - Accuracy: 0.5178, Train Time: 913.01 sec, Prediction Time: 76.12 sec
