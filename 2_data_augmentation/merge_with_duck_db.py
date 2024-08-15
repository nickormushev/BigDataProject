import duckdb
import sys 
import time

base_path = '/d/hpc/projects/FRI/bigdata/students/nk93594/'

if len(sys.argv) < 3:
    print("Usage: python merge_with_duck_db.py <category> <compression>")
    sys.exit(1)

category = sys.argv[1]
compression = sys.argv[2]

fileParking = 'dataset.parquet/*.parquet'
fileOther = f'{category}.parquet'

start = time.time()
conn = duckdb.connect()

# For elements where street_code1 is not 0 merge with it
query1 = ("SELECT * "
          f"FROM (SELECT * FROM parquet_scan('{base_path}{fileParking}', hive_partitioning = true) WHERE street_code1 != 0) a "
          f"LEFT JOIN parquet_scan('{base_path}{fileOther}') b "
          "ON a.street_code1 = b.street_code AND a.violation_county = b.borough ")

# Otherwise use street_code2
query2 = ("SELECT * "
          f"FROM (SELECT * FROM parquet_scan('{base_path}{fileParking}', hive_partitioning = true) WHERE street_code1 = 0) a "
          f"LEFT JOIN parquet_scan('{base_path}{fileOther}') b "
          "ON a.street_code2 = b.street_code AND a.violation_county = b.borough ")

# The union runs out of memory sadly
#query = f"COPY ({query1} UNION {query2}) TO '{base_path}dataset_with_{category}_duck.parquet' (FORMAT 'parquet', COMPRESSION 'snappy')"

# I tried to write only query1 but the output file was huge likely due to issue: Too large parquet files via "COPY TO"
query = f"COPY ({query1}) TO '{base_path}dataset_with_{category}_duck.parquet' (FORMAT 'parquet', COMPRESSION '{compression}')"

conn.execute(query)
end = time.time()
print(f"Execution Time with file: {end - start}")

