import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
cluster = SLURMCluster(cores=5, processes=1, memory="80GB")
client = Client(cluster)

cluster.scale(jobs=5)

# Define the base path of the Parquet file
base_path = '/d/hpc/projects/FRI/bigdata/students/cb17769/'
parquet_file = base_path + 'cleaned_data.parquet'

# Read the Parquet file into a Dask DataFrame
df = dd.read_parquet(parquet_file)

# Shuffle the data and take a 1% sample
sample = df.sample(frac=0.01, random_state=42).compute()

# Save the 1% sample as a new Parquet file
sample.to_parquet('/d/hpc/projects/FRI/bigdata/students/cb17769/sample_data.parquet')