import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=5, processes=1, memory="60GB")
client = Client(cluster)
cluster.scale(jobs=5)

# Define the base path of the Parquet file
base_path = '/d/hpc/projects/FRI/bigdata/students/cb17769/'

# Read the Parquet file into a Dask DataFrame
dataset = dd.read_parquet(f'{base_path}dataset.parquet')

length_dataset = dataset.shape[0].compute()
print(f"Dataset has {length_dataset} rows")
# 140423664

# convert the date to the same format as the weather data
dataset['issue_date'] = dd.to_datetime(dataset["issue_date"], format="mixed")
# add M to the end of the time to read it from 12 hour format
dataset['violation_time'] = dataset["violation_time"].str.upper() + "M"
# replace the values that starts with 00 to 12
dataset['violation_time'] = dataset['violation_time'].str.replace(r'^00', '12', regex=True)
# convert the time to 24 hour format
dataset['violation_time'] = dd.to_datetime(dataset["violation_time"], format="%I%M%p", errors="coerce")
# combine the date and time
dataset['issue_date'] = dd.to_datetime(dataset["issue_date"].dt.strftime('%Y-%m-%d') + ' ' + dataset["violation_time"].dt.strftime('%H:%M:%S'))
dataset = dataset.drop(["violation_time"], axis=1)

# Vehicle expiration date
dataset['vehicle_expiration_date'] = dd.to_datetime(dataset['vehicle_expiration_date'], format='%Y%m%d', errors='coerce')

# Fist observation datetime
dataset['time_first_observed'] = dataset["time_first_observed"].str.upper() + "M"
dataset['time_first_observed'] = dataset['time_first_observed'].str.replace(r'^00', '12', regex=True)
dataset['time_first_observed'] = dd.to_datetime(dataset['time_first_observed'], format='%I%M%p', errors='coerce')

# Date first observed
# replace 0 with NaN
dataset['date_first_observed'] = dataset['date_first_observed'].replace('0', None)
# replace 0001-01-03T12:00:00.000 with NaN
dataset['date_first_observed'] = dataset['date_first_observed'].replace('0001-01-03T12:00:00.000', None)

dataset['date_first_observed'] = dd.to_datetime(dataset['date_first_observed'], format='%Y%m%d', errors='coerce')

# merge the date and time
dataset['date_first_observed'] = dd.to_datetime(dataset["date_first_observed"].dt.strftime('%Y-%m-%d') + ' ' + dataset["time_first_observed"].dt.strftime('%H:%M:%S'))
dataset = dataset.drop(["time_first_observed"], axis=1)

# translate the county names to the borough names
county_to_borough = {
    "BRONX": "BX", # Bronx
    "BX": "BX",
    "Bronx": "BX",
    "BRONX": "BX",
    "BK": "K", # Brooklyn known as Kings
    "K": "K",
    "Kings": "K",
    "KINGS": "K",
    "KING": "K",
    "Q": "Q", # Queens
    "QN": "Q",
    "Qns": "Q",
    "QUEEN": "Q",
    "QUEENS": "Q",
    "QNS": "Q",
    "QU": "Q",
    "NY": "NY", # Manhattan known as New York
    "MN": "NY",
    "MAN": "NY",
    "NEW Y": "NY",
    "NEWY": "NY",
    "NYC": "NY",
    "ST": "R", # Staten Island known as Richmond
    "R": "R",
    "Rich": "R",
    "RICH": "R",
    "RICHM": "R",
    "RC": "R",
    "MH": "NY",
    "MS": "NY",
    "N": "NY",
    "P": "NY",
    "PBX": "NY",
    "USA": "NY",
    "VINIS": "NY",
    "A": None,
    "F": None,
    "ABX": None,
    "108": None,
    "103": "R", # Staten Island zip code
    "00000": None,
    "K   F": "K",
}

dataset['violation_county'] = dataset['violation_county'].replace(county_to_borough)

dataset = dataset.dropna(subset=['violation_county'])

borough_to_code = {
  'NY': 1,
  'BX': 2,
  'K': 3,
  'Q': 4,
  'R': 5
}

dataset['violation_county'] = dataset['violation_county'].replace(borough_to_code)
print("County names translated to borough names")

# compute
dataset = dataset.persist()
print("Data computed")

# Save the data as a new Parquet file
dataset.to_parquet(f'{base_path}cleaned_data.parquet', compression='snappy')
print("Data cleaning complete")