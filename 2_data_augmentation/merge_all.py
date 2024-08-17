import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import time

cluster = SLURMCluster(cores=5, processes=1, memory="80GB")
client = Client(cluster)
cluster.scale(jobs=5)

base_path = '/d/hpc/projects/FRI/bigdata/students/cb17769/'

if __name__ == '__main__':
    
    print("Starting")
    start_time = time.time()

    dataset = dd.read_parquet(base_path + 'cleaned_data.parquet')

    dataset['street_code'] = dataset['street_code1'].where(dataset['street_code1'] != 0, dataset['street_code2'].where(dataset['street_code2'] != 0, dataset['street_code3'])).astype("string")
    dataset['issue_date'] = dd.to_datetime(dataset['issue_date'])
    
    all_datasets = ['events', 'hs', 'attr', 'biz', 'weather']
    datasets_to_merge = ['weather']

    for pkName in datasets_to_merge:
        mergeDf = dd.read_parquet(base_path + f'augmented_data/{pkName}.parquet')

        print(f"Merging {pkName}")

        if pkName == 'events':
            # Merging events on borough and date
            merge_left_on = ['violation_county', 'issue_date']
            merge_right_on = ['borough', 'date']
            mergeDf['date'] = dd.to_datetime(mergeDf['date'])

        elif pkName == 'hs':
            # Merging schools on borough, street and year
            merge_left_on = ['violation_county', 'street_code', 'DataYear']
            merge_right_on = ['borough', 'street_code', 'DataYear']
            # Convert street_code to string
            mergeDf['street_code'] = mergeDf['street_code'].astype("string")

        elif pkName == 'weather':
            # Merging weather on borough and date
            merge_left_on = ['violation_county', 'issue_date']
            merge_right_on = ['borough', 'date']
            mergeDf['date'] = dd.to_datetime(mergeDf['date'])
            
            dataset['_date'] = dataset['issue_date'].dt.date
            dataset['_hour'] = dataset['issue_date'].dt.hour.fillna(0).astype(int)
        else: 
            # Merging attr and biz on borough and street
            merge_left_on = ['violation_county', 'street_code']
            merge_right_on = ['borough', 'street_code']
            # Convert street_code to string
            mergeDf['street_code'] = mergeDf['street_code'].astype("string")
        
        # Perform the merge
        dataset = dataset.merge(mergeDf, left_on=merge_left_on, right_on=merge_right_on, how='left', suffixes=('', f'_{pkName}'))

        # Drop the columns from the right DataFrame and the columns that start with '_'
        columns_to_drop = [col for col in dataset.columns if col.endswith(f'_{pkName}')] + \
                            [col for col in dataset.columns if col.startswith("_")]
        
        dataset = dataset.drop(columns_to_drop, axis=1)

        dataset = dataset.persist()

    print("Time before saving parquet: ", time.time() - start_time)

    compression = 'snappy'
    print(f"Saving parquet with compression {compression}")
    merged_datasets_string = '_'.join(datasets_to_merge)
    dataset.to_parquet(base_path + f'dataset_with_{merged_datasets_string}.parquet', compression=compression)

    print(f"Time taken: {time.time() - start_time}")

client.close()
cluster.close()

# Time to run:
# weather:  542 seconds
#        -> no saving 1.4 sec
# hs: 316 seconds
#        -> no saving 1.1 sec
# events: 410 seconds
#        -> no saving 4.9 sec
# attr: 586 seconds
#        -> no saving 5.9 sec 
# biz:  663 seconds
#        -> no saving 5.6 sec
# merging all:  1831 seconds
#        -> saving 14 sec