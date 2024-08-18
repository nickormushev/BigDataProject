import duckdb
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys


base_dir_nk = '/d/hpc/projects/FRI/bigdata/students/nk93594/'
directory = '/d/hpc/projects/FRI/bigdata/students/nk93594/dataset.parquet/'


if __name__ == '__main__':
    print('job started!')
    def visualize_bar(data, x, y, xlabel, ylabel, title, sv_name,sv_fig = True):

        if not os.path.exists('./fig'):
            os.makedirs('./fig')

        #  visuzalization
        plt.figure(figsize=(12, 8))
        sns.barplot(data = data, x = x, y = y, palette='viridis')
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        
        # save figure
        if sv_fig == True:
            plt.savefig(f'./fig/{sv_name}')
            
        plt.show()

    client = Client(
            n_workers=1,
            memory_limit="64GB",
            local_directory="/tmp",
        )
    print('Client is created!')

    print('Options to pick for att_name: biz, events, hs, attr')
    att_name = sys.argv[1]
    
    print('Options for DuckDB: True, False')
    duck = sys.argv[2]
    
    if att_name == 'biz':
        
        df_biz = dd.read_parquet(base_dir_nk + f'dataset_with_{att_name}.parquet')
        # Top 10 Industries for Violations
        df_ind = df_biz.groupby('Industry').size().compute().reset_index(name='counts')
        
        df_ind = df_ind.sort_values('counts', ascending=False).head(10)
        
        visualize_bar(df_ind, 'counts', 'Industry', 'Counts', 'Industry', 'Top 10 Number of Violations by Industry', 'by_industry.png')
        print('job is done')
    elif att_name == 'events':
        
        df_ev = dd.read_parquet(base_dir_nk + f'dataset_with_{att_name}.parquet')
        
        # Top 10 event_types
        df_et = df_ev.groupby('event_type').size().compute().reset_index(name='counts')
    
        df_et = df_et.sort_values('counts', ascending = False).head(10)
        
        visualize_bar(df_et, 'counts','event_type', 'Counts','Event Type', 'Top 10 Number of Violations by Event Type', 'by_event_type.png')
        
        # Is police precinct related to number of tickets?
        df_ev['police_precinct'] = df_ev['police_precinct'].fillna('').compute()
        pr = [len(pr.split()) for pr in df_ev['police_precinct']]
        pr_df = pd.DataFrame.from_dict(dict((pr, pr.count()) for i in set(pr)), orient = 'index', columns = ['counts'])
        pr_df = pr_df.sort_values('counts', ascending = False).head(10)
        
        visualize_bar(pr_df, pr_df.index, 'counts', 'Num. of Police Precinct', 'Counts', 'Num. of Violations by Num. of Police Precinct',
                      'by_pol_pre.png')
        
        
    elif att_name == 'hs':
        
        df_hs = dd.read_parquet(base_dir_nk + f'dataset_with_{att_name}.parquet')
        # Count the number of violations per borough
        count_br = df_hs.groupby('borough').size().compute().reset_index(name='counts')

        # Sum of total students per borough
        total_st = df_hs.groupby('borough')['total_students'].sum().compute().reset_index(name='total_students')

        # Merge the counts and total students DataFrames
        merged_df = count_br.merge(total_st, how='left', on='borough')

        # Plotting
        plt.figure(figsize=(12,8)) 
        sns.scatterplot(data=merged_df, x='total_students', y='counts', hue='borough', size='counts', palette='viridis')
        plt.title('Num. of Violations vs. Num. of Students per Borough')
        plt.xlabel('Num. of Students')
        plt.ylabel('Num. of Violations')

        # Save the plot
        plt.savefig('./fig/students_vs_violations.png')
        
    # attr
    elif att_name == 'attr':
        
        att_df = duckdb.sql(f'''SELECT DISTINCT Tourist_Spot,  COUNT(Tourist_Spot) as Counts FROM
                  '{base_dir_nk}dataset_with_attr.parquet/*.parquet'
                  GROUP BY Tourist_Spot  ORDER BY COUNT(Tourist_Spot) DESC LIMIT 10''').df()
        
        visualize_bar(att_df, 'Counts', 'Tourist_Spot', 'Counts', 'Tourist Spot', 'Num. of Violations per Tourist Spot',
                      'attr.png')
    else:
        print('Analyze will continue with original data.')


    df = dd.read_parquet(directory)
    
    # Filter data for 'P' and extract hour (assuming 'violation_time' is a string)
    df_filtered = df[df['violation_time'].str.contains('P', na=False)]
    df_filtered['hour_pm'] = df_filtered['violation_time'].str[:2]

    # Compute the result and convert to Pandas DataFrame
    df_computed = df_filtered['hour_pm'].value_counts().compute()
    hour_pm_df = pd.DataFrame(df_computed).reset_index()
    hour_pm_df.columns = ['hour', 'count']
    hour_pm_df = hour_pm_df.sort_values('hour')

    # Print DataFrame
    print(hour_pm_df)

    # Visualization
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=hour_pm_df, x='hour', y='count', palette='viridis')
    plt.xlabel('Hours (AM)')
    plt.ylabel('Count')
    plt.title('Number of Tickets per hour (AM)')
    plt.savefig('./fig/per_pm.png')
    plt.show()
   
    
    # Filter data for 'A' (AM) and extract hour part
    df_am_filtered = df[df['violation_time'].str.contains('A', na=False)]
    df_am_filtered['hour_am'] = df_am_filtered['violation_time'].str[:2]

    # Compute the result and convert to Pandas DataFrame
    hour_am_counts = df_am_filtered['hour_am'].value_counts().compute()
    df_hour_am = pd.DataFrame(hour_am_counts).reset_index()
    df_hour_am.columns = ['hour', 'count']
    df_hour_am = df_hour_am.sort_values('hour')

    # Print DataFrame
    print(df_hour_am)

    # Visualization
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_hour_am, x='hour', y='count', palette='viridis')
    plt.xlabel('Hours (AM)')
    plt.ylabel('Count')
    plt.title('Number of Tickets per hour (AM)')
    plt.savefig('./fig/per_am.png')
    plt.show()	
    
    # Analyze for intersections
    def get_inter(df):
        filter = df[df['street_name'] == 'Broadway']['intersecting_street'].dropna().str.split().str.get(-2)
        return filter
    
    bw_df = df.map_partitions(get_inter)
    bw_df = bw_df.value_counts().compute()
    bw_df.sort_values(ascending=False)[:10]
    
    # Top 10 intersecting streets for Broadway
    print(bw_df)    
    
    # Analyses using DuckDB
    if duck == 'True':
        
        # number of samples
        print('Number of Sampels:')
        print(duckdb.sql(f"""SELECT count(*) from '{directory}/part.*.parquet'
                """).df())
            
            
        # top 10 vehicles makes the most tickets
        print('Top 10 vehicle makes the most tickets:')
        top_vehicles = duckdb.sql(f"""SELECT vehicle_make, COUNT(*) AS num_tickets
                    FROM '{directory}/part.*.parquet'
                    GROUP BY vehicle_make
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                """).df()
        print(top_vehicles)

        visualize_bar(top_vehicles, 'num_tickets', 'vehicle_make', 'Number of Tickets',
                'Vehicle Make', 'Top 10 Vehicle Makes with Most Tickets','top_10_vehicle.png')


        # Number of Tickets per year
        print('Number of tickets per year:')
        per_year = duckdb.sql(f"""SELECT DataYear, COUNT(*) AS num_tickets
                    FROM '{directory}/part.*.parquet'
                    GROUP BY DataYear
                    ORDER BY COUNT(*) DESC
                """).df()
        print(per_year)

        #  visuzalization
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=per_year.sort_values('DataYear'), x='DataYear', y='num_ticket', palette='viridis')
        plt.xlabel('Number of Tickets')
        plt.ylabel('Years')
        plt.title('Number of Tickets per year')
        plt.savefig('./fig/per_year.png')
        plt.show()

        # Most common violation types
        print('Most common violation types:')
        most_common = duckdb.sql(f"""
                SELECT violation_description, COUNT(*) AS num_violations
                FROM '{directory}/part.*.parquet'
                GROUP BY violation_description
                ORDER BY num_violations DESC
                LIMIT 10
                """).df()
        print(most_common)

        visualize_bar(most_common, 'num_violations', 'violation_description',
                    'Number of Violations', 'Violation Description', 'Top 10 Most Common Violation Types',
                    './fig/most_common.png')

        # top streets per number of violations
        print('Top streets:')
        top_streets = duckdb.sql(f"""SELECT street_name, COUNT(*) AS num_violations
        FROM '{directory}/part.*.parquet'
        GROUP BY street_name
        ORDER BY num_violations DESC
        LIMIT 10""").df()

        print(top_streets)

        visualize_bar(top_streets, 'num_violations', 'street_name',
                    'Number of Violations', 'Street Name',
                    'Top 10 Streets with Most Violations', './fig/top_street.png')
    else:
        pass

    client.close()
