import dask.dataframe as dd
from dask.distributed import Client
import sys
import pandas as pd
import numpy as np


base_path = '/d/hpc/projects/FRI/bigdata/students/nk93594/'

if __name__ == '__main__':
    client = Client(
            n_workers=1,
            memory_limit="60GB",
            local_directory="/tmp",
        )

    pkName = sys.argv[1]
    compression = sys.argv[2]

    pk = dd.read_parquet(base_path + 'dataset_2.parquet')
    mergeDf = dd.read_parquet(base_path + f'{pkName}.parquet')

    pk['street_code1or2'] = pk['street_code1'].where(pk['street_code1'] != 0, pk['street_code2'].where(pk['street_code2'] != 0, pk['street_code3'])).astype("string")
    mergeDf['street_code'] = mergeDf['street_code'].astype("string")

    print(f"Merging {pkName}")

    if pkName == 'events':
        # Events need to merge also on the date
        # I do not use street_code because it does not match. We will look at how it affects the whole 
        pk = pk.sample(frac=0.7, random_state=42)
        merged = pk.merge(mergeDf, left_on=['violation_county', 'issue_date'], right_on=['borough', 'date'], how='left')
    elif pkName == 'hs':
        merged = pk.merge(mergeDf, left_on=['violation_county', 'street_code1or2', 'DataYear'], right_on=['borough', 'street_code', 'DataYear'], how='left')

    elif pkName == 'weather':
        pk['issue_date'] = dd.to_datetime(pk['issue_date'])
        pk['_date'] = pk['issue_date'].dt.date
        pk['_hour'] = pk['issue_date'].dt.hour
        merged = pk.merge(mergeDf, left_on=['_date', '_hour', 'violation_county'], right_on=['date', 'hour', 'borough'], how='left')

        merged = merged.drop(['_date', '_hour'], axis=1)
    else:
        merged = pk.merge(mergeDf, left_on=['violation_county', 'street_code1or2'], right_on=['borough', 'street_code'], how='left')


    print(f"Saving parquet with compression {compression}")
    merged.to_parquet(base_path + f'dataset_with_{pkName}.parquet', compression=compression)


    print("Closing client")
    client.close()