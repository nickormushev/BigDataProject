import dask.dataframe as dd

from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import dask.array as da

import dask_ml.preprocessing as dpreprocessing
from dask_ml.model_selection import train_test_split
from dask_ml.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, mean_squared_error
import dask_xgboost as dxgb
import xgboost as xgb
import numpy as np

import time
import os
import sys

base_dir_nk = '/d/hpc/projects/FRI/bigdata/students/nk93594/'
directory = '/d/hpc/projects/FRI/bigdata/students/nk93594/dataset.parquet/'


if __name__ == '__main__':
    
    prob = sys.argv[1]
    
    print('Starting..')
    print('Slurm Cluster Building...')
    cluster = SLURMCluster(cores=5, processes=1, memory="60GB")
    print('SlumCluster is ready.')
    client = Client(cluster)
    print('Client is ready.')
    cluster.scale(jobs=5)
    print('Cluster is scaled')
    
    df = dd.read_parquet(directory, assume_missing = True)
    print('Dataread.')
    
    # Perform preprocessing
    df = df.dropna()  # Handle missing values 
    print('Missing values handled.')
    
    # High Ticket days
    if prob == '1':
        
        # Aggregating tickets by day
        print('Aggregation started')
        daily_tickets = df.groupby('issue_date').size().reset_index()
        daily_tickets.columns = ['issue_date', 'ticket_count']
        print('Aggregation ended')

        # Creating a binary target for high number of tickets (e.g., above median)
        print('Binary values are creating.')
        median_ticket_count = daily_tickets['ticket_count'].median().compute()
        daily_tickets['high_ticket_day'] = daily_tickets['ticket_count'] > median_ticket_count
        
        # Example preprocessing: Extracting features (this will depend on your actual dataset structure)
        print('Preprocessing started.')
        daily_tickets['issue_date'] = dd.to_datetime(daily_tickets['issue_date'])
        daily_tickets['day_of_week'] = daily_tickets['issue_date'].dt.dayofweek
        daily_tickets['month'] = daily_tickets['issue_date'].dt.month
        print('Proprocessing ended.')

        print('Data is splitting.')
        # Splitting the data into features and target
        X = daily_tickets[['day_of_week', 'month']].values
        y = daily_tickets['high_ticket_day'].values

        print('Convertion to dask array.')
        # Convert to Dask arrays for distributed processing
        X_dask = da.from_array(X, chunks=(1000, X.shape[1]))
        y_dask = da.from_array(y, chunks=(1000, ))
        print('Convertion ended.')
        
        def dask_ml_logistic_regression(X, y):
        
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            clf = LogisticRegression()
            start_time = time.time()
            clf.fit(X_train, y_train)
            elapsed_time = time.time() - start_time

            y_pred = clf.predict(X_test).compute()
            accuracy = accuracy_score(y_test.compute(), y_pred)

            return accuracy, elapsed_time
    
        def dask_ml_logistic_regression(X, y):
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            clf = LogisticRegression()
            start_time = time.time()
            clf.fit(X_train, y_train)
            elapsed_time = time.time() - start_time

            y_pred = clf.predict(X_test).compute()
            accuracy = accuracy_score(y_test.compute(), y_pred)

            return accuracy, elapsed_time
        
        def dask_xgboost(X, y):
            
            dtrain = dxgb.DaskDMatrix(client, X, y)

            params = {
                'objective': 'reg:squarederror',
                'learning_rate': 0.1,
                'max_depth': 6,
                'n_estimators': 100
            }

            start_time = time.time()
            bst = dxgb.train(client, params, dtrain, num_boost_round=100)
            elapsed_time = time.time() - start_time

            y_pred = bst.predict(dxgb.DaskDMatrix(client, X))
            rmse = mean_squared_error(y.compute(), y_pred.compute(), squared=False)

            return rmse, elapsed_time
        
        def sklearn_sgd_partial_fit(X, y):
            
            model = make_pipeline(StandardScaler(), SGDClassifier(max_iter=1000, tol=1e-3))

            classes = np.array([0, 1])
            start_time = time.time()

            for i in range(0, len(X), 1000):
                X_batch = X[i:i+1000].compute()
                y_batch = y[i:i+1000].compute()
                model.named_steps['sgdclassifier'].partial_fit(X_batch, y_batch, classes=classes)

            elapsed_time = time.time() - start_time

            y_pred = model.predict(X.compute())
            accuracy = accuracy_score(y.compute(), y_pred)

            return accuracy, elapsed_time
        
        print("Evaluating Dask-ML Logistic Regression...")
        log_reg_accuracy, log_reg_time = dask_ml_logistic_regression(X_dask, y_dask)
        print(f"Accuracy: {log_reg_accuracy:.4f}, Time: {log_reg_time:.2f} seconds")

        print("Evaluating Dask-XGBoost...")
        xgb_rmse, xgb_time = dask_xgboost(X_dask, y_dask)
        print(f"RMSE: {xgb_rmse:.4f}, Time: {xgb_time:.2f} seconds")

        print("Evaluating Scikit-Learn SGDClassifier with partial_fit...")
        sgd_accuracy, sgd_time = sklearn_sgd_partial_fit(X_dask, y_dask)
        print(f"Accuracy: {sgd_accuracy:.4f}, Time: {sgd_time:.2f} seconds")
        
        client.close()
        cluster.close()
        
    # Violation code
    if prob == '2':
        # Preprocess data
        df['issue_date'] = dd.to_datetime(df['issue_date'])
        df['day_of_week'] = df['issue_date'].dt.dayofweek
        df['hour'] = df['issue_date'].dt.hour
        df['time_of_day'] = df['hour'].apply(lambda x: 'morning' if 6 <= x < 12 
                                            else 'afternoon' if 12 <= x < 18 
                                            else 'evening' if 18 <= x < 24 
                                            else 'night', meta=('x', 'str'))

        # Categorize categorical columns
        df = df.categorize(columns=['borough', 'precinct', 'street_name', 'time_of_day', 'vehicle_make'])

        # Prepare features and target
        X = df[['borough', 'precinct', 'street_name', 'day_of_week', 'time_of_day', 'vehicle_make']]
        y = df['violation_code']

        # Encode categorical features
        encoder = dpreprocessing.Categorizer()
        X = encoder.fit_transform(X)

        # Split data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Convert to Dask Array for XGBoost
        X_train_da = X_train.to_dask_array(lengths=True)
        y_train_da = y_train.to_dask_array(lengths=True)

       # Function for Dask-ML Logistic Regression
        def train_dask_logistic_regression(X_train, y_train, X_test, y_test):
            start_time = time.time()
            log_reg = LogisticRegression()
            log_reg.fit(X_train, y_train)
            train_time = time.time() - start_time
            
            start_time = time.time()
            y_pred = log_reg.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred).compute()
            prediction_time = time.time() - start_time
            
            return accuracy, train_time, prediction_time
        
        
        # Function for XGBoost
        def train_xgboost(X_train, y_train, X_test, y_test):
            X_train_da = X_train.to_dask_array(lengths=True)
            y_train_da = y_train.to_dask_array(lengths=True)
            
            start_time = time.time()
            params = {
                'objective': 'multi:softmax',
                'num_class': len(y.unique().compute()),
                'learning_rate': 0.1,
                'max_depth': 6,
                'n_estimators': 100
            }
            
            dtrain = dxgb.DaskDMatrix(client, X_train_da, y_train_da)
            bst = dxgb.train(client, params, dtrain, num_boost_round=100)
            train_time = time.time() - start_time
            
            start_time = time.time()
            y_pred = bst.predict(dxgb.DaskDMatrix(client, X_test.to_dask_array(lengths=True)))
            accuracy = accuracy_score(y_test, y_pred).compute()
            prediction_time = time.time() - start_time
            
            return accuracy, train_time, prediction_time
        
        # Function for Scikit-Learn SGD Classifier with partial_fit
        def train_sgd_partial_fit(X_train, y_train, X_test, y_test):
            sgd = SGDClassifier(loss='log', max_iter=1000, tol=1e-3)
            classes = y.unique().compute().astype('int')
            
            start_time = time.time()
            for i in range(0, len(X_train), 1000):
                X_batch = X_train[i:i+1000].compute()
                y_batch = y_train[i:i+1000].compute()
                sgd.partial_fit(X_batch, y_batch, classes=classes)
            train_time = time.time() - start_time
            
            start_time = time.time()
            y_pred = sgd.predict(X_test.compute())
            accuracy = accuracy_score(y_test.compute(), y_pred)
            prediction_time = time.time() - start_time
            
            return accuracy, train_time, prediction_time

        # Running and comparing all models
        log_reg_accuracy, log_reg_train_time, log_reg_pred_time = train_dask_logistic_regression(X_train, y_train, X_test, y_test)
        print(f"Dask-ML Logistic Regression - Accuracy: {log_reg_accuracy:.4f}, Train Time: {log_reg_train_time:.2f} sec, Prediction Time: {log_reg_pred_time:.2f} sec")

        xgb_accuracy, xgb_train_time, xgb_pred_time = train_xgboost(X_train, y_train, X_test, y_test)
        print(f"XGBoost - Accuracy: {xgb_accuracy:.4f}, Train Time: {xgb_train_time:.2f} sec, Prediction Time: {xgb_pred_time:.2f} sec")

        sgd_accuracy, sgd_train_time, sgd_pred_time = train_sgd_partial_fit(X_train, y_train, X_test, y_test)
        print(f"SGD Classifier - Accuracy: {sgd_accuracy:.4f}, Train Time: {sgd_train_time:.2f} sec, Prediction Time: {sgd_pred_time:.2f} sec")
        
        client.close()
        cluster.close()
