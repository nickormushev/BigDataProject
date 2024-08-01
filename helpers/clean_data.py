import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=5, processes=1, memory="60GB")
client = Client(cluster)
cluster.scale(jobs=5)

# Define the base path of the Parquet file
base_path = '/d/hpc/projects/FRI/bigdata/students/cb17769/'
parquet_file = base_path + 'dataset.parquet'

# Read the Parquet file into a Dask DataFrame
sample = dd.read_parquet(parquet_file)

# drop data year because it is wrong
sample.drop("DataYear", axis=1)
print("Data year column dropped")

# convert the date to the same format as the weather data
sample['issue_date'] = dd.to_datetime(sample["issue_date"], format="mixed")
print("Date format converted")

# translate the county names to the borough names
county_to_borough = {
    "BRONX": "bronx",
    "BX": "bronx",
    "Bronx": "bronx",
    "BRONX": "bronx",
    "BK": "brooklyn",
    "K": "brooklyn",
    "Kings": "brooklyn",
    "KINGS": "brooklyn",
    "KING": "brooklyn",
    "Q": "queens",
    "QN": "queens",
    "Qns": "queens",
    "QUEEN": "queens",
    "QUEENS": "queens",
    "QNS": "queens",
    "QU": "queens",
    "NY": "manhattan",
    "MN": "manhattan",
    "MAN": "manhattan",
    "NEW Y": "manhattan",
    "NEWY": "manhattan",
    "NYC": "manhattan",
    "ST": "staten_island",
    "R": "staten_island",
    "Rich": "staten_island",
    "RICH": "staten_island",
    "RICHM": "staten_island",
    "RC": "staten_island",
    "MH": "manhattan",
    "MS": "manhattan",
    "N": "manhattan",
    "P": "manhattan",
    "PBX": "manhattan",
    "USA": "manhattan",
    "VINIS": "manhattan",
    "A": "unknown",
    "F": "unknown",
    "ABX": "unknown",
    "108": "unknown",
    "103": "unknown",
    "00000": "unknown",
    "K   F": "unknown"
}

sample['violation_county'] = sample['violation_county'].map(county_to_borough)
print("County names translated to borough names")

# compute
sample = sample.persist()
print("Data computed")

# Save the data as a new Parquet file
sample.to_parquet('/d/hpc/projects/FRI/bigdata/students/cb17769/cleaned_data.parquet')
print("Data cleaning complete")