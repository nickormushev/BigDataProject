import dask.dataframe as dd
import sys 
import time 
from dask.distributed import Client

base_path = '/d/hpc/projects/FRI/bigdata/students/nk93594/'

fileParquet = f'dataset.parquet'
fileHDF = f'dataset.hd5' 

if __name__ == '__main__':
    # HDF
    client = Client(
            n_workers=1,
            memory_limit="60GB",
            local_directory="/tmp",
        )

    start = time.time()
    df = dd.read_hdf(f'{base_path}{fileHDF}', key='data')
    df_filtered = df[df.street_code1 != 0]

    count = df_filtered.count().compute()
    print(count)
    end = time.time()

    print(f"Execution Time for hdf: {end - start}")

    # Parquet
    start = time.time()
    df = dd.read_parquet(f'{base_path}{fileParquet}')

    df_filtered = df[df.street_code1 != 0]

    count = df_filtered.count().compute()
    print(count)
    end = time.time()

    print(f"Execution Time for parquet: {end - start}")

    client.close()
