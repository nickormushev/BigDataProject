import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import os

base_path = '/d/hpc/projects/FRI/bigdata/students/nk93594/'
parquet_file = base_path + 'dataset.parquet'
dtype_spec = {
    "summons_number": "int64", "plate_id": "str", "registration_state": "str", "plate_type": "str",
    "violation_code": "int64", "vehicle_body_type": "str", "vehicle_make": "str", "issuing_agency": "str",
    "street_code1": "int64", "street_code2": "int64", "street_code3": "int64",
    "vehicle_expiration_date": "str", "violation_location": "str", "violation_precinct": "str",
    "issuer_precinct": "str", "issuer_code": "str", "issuer_command": "str", "issuer_squad": "str",
    "violation_time": "str", "time_first_observed": "str", "violation_county": "str",
    "violation_in_front_of_or_opposite": "str", "house_number": "str", "street_name": "str",
    "intersecting_street": "str", "date_first_observed": "str", "law_section": "str",
    "sub_division": "str", "violation_legal_code": "str", "days_parking_in_effect": "str",
    "from_hours_in_effect": "str", "to_hours_in_effect": "str", "vehicle_color": "str",
    "unregistered_vehicle": "str", "vehicle_year": "str", "meter_number": "str",
    "feet_from_curb": "str", "violation_post_code": "str", "violation_description": "str",
    "no_standing_or_stopping_violation": "str", "hydrant_violation": "str",
    "double_parking_violation": "str"
}

# Used for comparison to see if I have columns to rename
def group_csv_files_by_type():

    csv_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]

    first_lines = {}
    for file in csv_files:
        df = pd.read_csv(os.path.join(base_path, file), nrows=1)
        first_lines[file] = tuple(df.columns.tolist())  # Convert to tuple

    groups = {}
    for file, first_line in first_lines.items():
        if first_line not in groups:
            groups[first_line] = []
        groups[first_line].append(file)

    for first_line, files in groups.items():
        print(f"Files with first line {first_line}:")
        for file in files:
            print(f"  {file}")


def convert_to_parquet(base_path, dtype_spec, parquet_file):
    df = dd.read_csv(base_path + '*.csv', dtype=dtype_spec)

    df['law_section'] = df['law_section'].fillna(0).astype('int64')
    df['vehicle_year'] = df['vehicle_year'].fillna(0).astype('int64')
    df['feet_from_curb'] = df['feet_from_curb'].fillna(0).astype('int64')
    df['violation_precinct'] = df['violation_precinct'].fillna(0).astype('int64')
    df['issuer_precinct'] = df['issuer_precinct'].fillna(0).astype('int64')
    df['issuer_code'] = df['issuer_code'].fillna(0).astype('int64')

    df.to_parquet(parquet_file)

def convert_to_hdf(parquet_file):
    df = dd.read_parquet(parquet_file).astype('object')
    df.repartition(npartitions=100000)
    df = df[df['DataYear'] == 2014]

    print(df.select_dtypes(include=['str']).columns)

    df.to_hdf(base_path + 'dataset.hdf', key = "data", mode = "w", min_itemsize={'violation_description': 50, 'plate_id': 50})

#client = Client(
#        n_workers=2,
#        threads_per_worker=2,
#        memory_limit="6.5GB",
#        local_directory="/tmp",
#    )


convert_to_hdf(parquet_file)


#client.close()