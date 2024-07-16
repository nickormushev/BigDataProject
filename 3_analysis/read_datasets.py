import duckdb
import sys 

base_path = '/d/hpc/projects/FRI/bigdata/students/nk93594/'

category = sys.argv[1]

file = f'dataset_with_{category}.parquet/*.parquet'


conn = duckdb.connect()
query = ("SELECT COUNT(*) "
         f"FROM parquet_scan('{base_path}{file}', hive_partitioning = true) "
         "WHERE street_code IS NOT NULL "
         "LIMIT 50")

result = conn.execute(query)
df = result.fetch_df()
print(df.head())