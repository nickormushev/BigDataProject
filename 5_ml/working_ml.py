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
import pandas as pd

import time
import os
import sys

base_dir_nk = '/d/hpc/projects/FRI/bigdata/students/nk93594/'
base_dir_cb = '/d/hpc/projects/FRI/bigdata/students/cb17769/'

cleaned_dataset_dir = base_dir_cb + 'cleaned_data.parquet/'
weather_dataset_dir = base_dir_cb + 'augmented_data/weather.parquet'
events_dataset_dir = base_dir_cb + 'augmented_data/events.parquet'
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
        'num_class': num_class if num_class > 2 else None,
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
    
    n_workers = 8
    memory_limit = 8
    cluster = SLURMCluster(cores=5, # Number of cores per job
                           processes=1, 
                           memory=f"{memory_limit}GB", # Memory per job
                           job_extra_directives=['--time=06:00:00']
                        )
    client = Client(cluster)
    cluster.scale(n=n_workers) # Ask for workers
    
    df = dd.read_parquet(cleaned_dataset_dir, assume_missing = True)
    
    # High Ticket days
    if prob == '1':
        print('Solving problem 1: Predicting high ticket days')

        if os.path.exists(base_dir_cb + 'encoded_high_ticket_days.parquet'):
            daily_tickets = dd.read_parquet(base_dir_cb + 'encoded_high_ticket_days.parquet')
        else:
            # Aggregating tickets by day
            df['issue_date'] = dd.to_datetime(df['issue_date'])
            df['date'] = df['issue_date'].dt.date

            # Events data
            event_data = dd.read_parquet(events_dataset_dir)
            event_data['date'] = dd.to_datetime(event_data['date']).dt.date
            daily_events = event_data.groupby(['borough', 'date']).size().reset_index()
            daily_events = daily_events.rename(columns={0: 'event_count'})

            # Tickets data
            daily_tickets = df.groupby(['violation_county', 'date']).size().reset_index()
            daily_tickets = daily_tickets.rename(columns={0: 'ticket_count'})

            # Merging events data with tickets data
            daily_tickets = daily_tickets.merge(daily_events, left_on=['violation_county', 'date'], right_on=['borough', 'date'], how='left')
            daily_tickets = daily_tickets.drop(['borough'], axis=1)
            

            # Weather data
            weather_data = dd.read_parquet(weather_dataset_dir)
            weather_data['date'] = dd.to_datetime(weather_data['date']).dt.date
            weather_data['is_rainy'] = weather_data['prcp'] > 0            
            daily_weather = weather_data.groupby(['borough', 'date'])['is_rainy'].max().reset_index()
            
            # Merging weather data with tickets data
            daily_tickets = daily_tickets.merge(daily_weather, left_on=['violation_county', 'date'], right_on=['borough', 'date'], how='left')
            daily_tickets = daily_tickets.drop(['borough'], axis=1)

            # Filling missing values
            daily_tickets['event_count'] = daily_tickets['event_count'].fillna(0)
            daily_tickets['is_rainy'] = daily_tickets['is_rainy'].fillna(False)

            del daily_events, daily_weather

            # Creating a binary target for high number of tickets (e.g., above median)
            median_ticket_count = daily_tickets['ticket_count'].median().compute()
            print('Median ticket count:', median_ticket_count)
            daily_tickets['high_ticket_day'] = daily_tickets['ticket_count'] > median_ticket_count
            
            # Preprocessing
            daily_tickets['date'] = dd.to_datetime(daily_tickets['date'])
            daily_tickets['day_of_week'] = daily_tickets['date'].dt.dayofweek
            daily_tickets['month'] = daily_tickets['date'].dt.month

            daily_tickets = daily_tickets[['violation_county', 'day_of_week', 'month', 'event_count', 'is_rainy', 'high_ticket_day']].reset_index(drop=True)

            # Categories and encode features
            categorizer = Categorizer(columns=['violation_county'])
            daily_tickets = categorizer.fit_transform(daily_tickets)

            dummy_encoder = DummyEncoder(columns=['violation_county'], drop_first=True)
            daily_tickets = dummy_encoder.fit_transform(daily_tickets)

            daily_tickets = daily_tickets.dropna()

            for col in daily_tickets.columns:
                daily_tickets[col] = daily_tickets[col].astype(int)

            # Save file for future use
            daily_tickets.to_parquet(base_dir_cb + 'encoded_high_ticket_days.parquet')

        X = daily_tickets.drop('high_ticket_day', axis=1).persist()
        y = daily_tickets['high_ticket_day'].persist()
        del daily_tickets

        

    # Violation code
    elif prob == '2':
        print('Solving problem 2: Predicting violation code')

        # Load encoded data if it exists
        if os.path.exists(base_dir_cb + 'encoded_violation_code.parquet'):
            df = dd.read_parquet(base_dir_cb + 'encoded_violation_code.parquet')
        else:
            # Preprocess data
            
            df['street_code'] = df['street_code1'].where(df['street_code1'] != 0, df['street_code2'].where(df['street_code2'] != 0, df['street_code3'])).astype("string")
            df['street_code'] = df['street_code'].replace({'0': None})
            
                        
            features = ['violation_county', 'street_code', 'issue_date', 'violation_code', 'vehicle_year', 'vehicle_expiration_date', 'plate_type', 'unregistered_vehicle', 'feet_from_curb', 'days_parking_in_effect']

            df = df[features]
            df = df.dropna()
            df['street_code'] = df['street_code'].astype(int)
            
            df['violation_code'] = df['violation_code'].fillna(0).astype(int)
            df['violation_code'] = df['violation_code'].replace({0: None})

            current_year = 2024
            df['vehicle_year'] = df['vehicle_year'].astype(int)
            df['vehicle_year'] = df['vehicle_year'].replace({0: None})
            df['vehicle_year'] = df['vehicle_year'].where(df['vehicle_year'] <= current_year, None)
            
            df['unregistered_vehicle'] = df['unregistered_vehicle'].notna() & (df['unregistered_vehicle'] == '0')
            df['unregistered_vehicle'] = df['unregistered_vehicle'].astype(int)
            df['feet_from_curb'] = df['feet_from_curb'].astype(int)

            # Extract features from issue_date
            df['issue_date'] = dd.to_datetime(df['issue_date'])
            df['day_of_week'] = df['issue_date'].dt.dayofweek
            df['hour'] = df['issue_date'].dt.hour
            df['hour'] = df['hour'].fillna(12)

            df['vehicle_expiration_date'] = pd.to_datetime(df['vehicle_expiration_date'], format='%Y%m%d', errors='coerce')
            df['is_expired'] = df['vehicle_expiration_date'] < df['issue_date']

            def check_plate_type(df):
                plate_types = "AGR Agricultural Vehicle MCD Motorcycle Dealer AMB Ambulance MCL Marine Corps League ARG Air National Guard MED Medical Doctor ATD All Terrain Deale MOT Motorcycle ATV All Terrain Vehicle NLM Naval Militia AYG Army National Guard NYA New York Assembly BOB Birthplace of Baseball NYC New York City Council BOT Boat NYS New York Senate CBS County Bd. of Supervisors OMF Omnibus Public Service CCK County Clerk OML Livery CHC  Household Carrier (Com) OMO Omnibus Out-of-State CLG County Legislators OMR Bus CMB Combination - Connecticut OMS Rental CME  Coroner Medical Examiner OMT Taxi CMH Congress. Medal of Honor OMV Omnibus Vanity COM Commercial Vehicle ORC Organization (Com) CSP Sports (Com) ORG Organization (Pas) DLR Dealer PAS Passenger EDU Educator PHS Pearl Harbor Survivors FAR Farm vehicle PPH Purple Heart FPW Former Prisoner of War PSD Political Subd. (Official) GAC Governor's Additional Car RGC Regional (Com) GFC Gift Certificate RGL Regional (Pas) GSC Governor's Second Car SCL School Car GSM Gold Star Mothers SNO Snowmobile HAC Ham Operator Comm SOS Survivors of the Shield HAM Ham Operator SPC Special Purpose Comm. HIF Special Reg.Hearse SPO Sports HIR Hearse Coach SRF Special Passenger - Vanity HIS Historical Vehicle SRN Special Passenger - Judges HOU House/Coach Trailer STA State Agencies HSM Historical Motorcycle STG State National Guard IRP Intl. Registration Plan SUP Justice Supreme Court ITP In Transit Permit TOW Tow Truck JCA Justice Court of Appeals TRA Transporter JCL Justice Court of Claims THC Household Carrier Tractor JSC Supreme Court Appl. Div TRC Tractor Regular JWV Jewish War Veterans TRL Trailer Regular LMA Class A Limited Use Mtrcyl. USC U. S. Congress LMB Class B Limited Use Mtrcyl. USS U. S. Senate LMC Class C Limited Use Mtrcyl. VAS Voluntary Ambulance Svc. LOC Locomotive VPL Van Pool LTR Light Trailer WUG World University Games LUA Limited Use Automobile"
                plate_types = plate_types.split()
                plate_types = [plate_id for plate_id in plate_types if plate_id.isupper() and len(plate_id) == 3]
                df['plate_type'] = df['plate_type'].apply(lambda x: x if x in plate_types else None)
                return df

            df['plate_type'] = df['plate_type'].map_partitions(check_plate_type)

            # Assume the days of the week correspond to each character position
            days_of_week = ['parking_monday', 'parking_tuesday', 'parking_wednesday', 'parking_thursday', 'parking_friday', 'parking_saturday', 'parking_sunday']

            # Function to split the string into individual True/False values for each day
            def split_parking_days(parking_days):
                if parking_days is None:
                    return {day: False for day in days_of_week}

                parking_days = parking_days.replace(' ', 'B')
                parking_days = parking_days.ljust(7, 'B')
                return {day: (char == 'Y') for day, char in zip(days_of_week, parking_days)}

            days_columns = df['days_parking_in_effect'].map_partitions(
                lambda df: df.apply(split_parking_days, axis=1)
            )
            days_columns = days_columns.map_partitions(lambda data: data.apply(pd.Series))
            df = df.join(days_columns)
            
            df = df[['violation_county', 'street_code', 'vehicle_year', 'plate_type', 'unregistered_vehicle', 'feet_from_curb', 'day_of_week', 'hour', 'is_expired', 'parking_monday', 'parking_tuesday', 'parking_wednesday', 'parking_thursday', 'parking_friday', 'parking_saturday', 'parking_sunday', 'violation_code']]
        
            # Categories and encode features
            categorizer = Categorizer(columns=['violation_county', 'plate_type'])
            df = categorizer.fit_transform(df)
            
            dummy_encoder = DummyEncoder(columns=['violation_county', 'plate_type'], drop_first=True)
            df = dummy_encoder.fit_transform(df)

            # Save file for future use
            df.to_parquet(base_dir_cb + 'encoded_violation_code.parquet')
        
        X = df.drop('violation_code', axis=1).persist()
        y = df['violation_code'].persist()
        del df

    print('Data loaded')
    print(X.head())
    print(y.head())
    
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
    log_reg_accuracy, log_reg_train_time, log_reg_pred_time = dask_ml_logistic_regression(X_train_da, y_train_da, X_test_da, y_test_da)
    print(f"Dask-ML Logistic Regression - Accuracy: {log_reg_accuracy:.4f}, Train Time: {log_reg_train_time:.2f} sec, Prediction Time: {log_reg_pred_time:.2f} sec")

    xgb_accuracy, xgb_train_time, xgb_pred_time = dask_xgboost(client, X_train_da, y_train_da, X_test_da, y_test_da)
    print(f"XGBoost - Accuracy: {xgb_accuracy:.4f}, Train Time: {xgb_train_time:.2f} sec, Prediction Time: {xgb_pred_time:.2f} sec")

    sgd_accuracy, sgd_train_time, sgd_pred_time = sklearn_sgd_partial_fit(X_train_da, y_train_da, X_test_da, y_test_da)
    print(f"SGD Classifier - Accuracy: {sgd_accuracy:.4f}, Train Time: {sgd_train_time:.2f} sec, Prediction Time: {sgd_pred_time:.2f} sec")
    
    client.close()
    cluster.close()


## RESULTS

# Solving problem 1: Predicting high ticket days
# Median ticket count: 8550.0
# Dask-ML Logistic Regression - Accuracy: 0.5206, Train Time: 7977.60 sec, Prediction Time: 0.01 sec
# XGBoost - Accuracy: 0.5437, Train Time: 63.91 sec, Prediction Time: 0.36 sec
# SGD Classifier - Accuracy: 0.5178, Train Time: 913.01 sec, Prediction Time: 76.12 sec

# Solving problem 1: Predicting high ticket days
# Median ticket count: 3273.0
# Dask-ML Logistic Regression - Accuracy: 0.7333, Train Time: 164.64 sec, Prediction Time: 0.02 sec
# XGBoost - Accuracy: 0.7741, Train Time: 3.98 sec, Prediction Time: 0.33 sec
# SGD Classifier - Accuracy: 0.5961, Train Time: 2.03 sec, Prediction Time: 0.17 sec