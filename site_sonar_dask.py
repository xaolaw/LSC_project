import os
import json
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
# import matplotlib.pyplot as plt

from dask.distributed import LocalCluster, Client, wait
from dask_mpi import initialize

import time

client = None

HAVE_DATA = False
USE_DASK_DATAFRAME = False

# Configure these two dirs (on Athena it is recommended to use $SCRATCH)
data_directory = '/net/tscratch/people/plgmszewc/downloads'
output_directory = '/net/tscratch/people/plgmszewc/temp' 

def count_time(name):
    def time_decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            wyn = func(*args, **kwargs)
            end_time = time.time()
            print(f'{name} time: {end_time - start_time}')
            return wyn
        return wrapper
    return time_decorator

# Extract parameters and flattens their names
def flatten_entry(entry, timestamp):
    flattened_entries = []
    common_fields = {
        'host_ce': f"{entry['hostname']}_{entry['ce_name']}",
        'date': datetime.fromtimestamp(timestamp).date()
    }
    
    for test, results in entry['test_results_json'].items():
        for key, value in results.items():
            # Exclude some keys, e.g. very lengthy *CONDOR* params
            if key not in ['EXITCODE', 'EXECUTION_TIME', 'CONDOR_MACHINE_CLASSAD_CONTENTS', 'CONDOR_JOB_CLASSAD_CONTENTS']:
                if isinstance(value, list) or isinstance(value, dict):  # Handle list or dict values by converting to string
                    value_to_write = json.dumps(value)
                else:
                    value_to_write = value
                flattened_entry = common_fields.copy()
                flattened_entry['param_name'] = f'{test}_{key}'
                flattened_entry['param_value'] = value_to_write
                flattened_entries.append(flattened_entry)
    
    return flattened_entries

@count_time("compute changes")
def compute_changes_across_entries(df):
    global output_directory
    groups = df.groupby(['host_ce', 'param_name'])
    df['changed'] = (df['param_value'].ne(df['param_value'].shift()) & df['param_name'].eq(df['param_name'].shift())).astype(int)
    print("Done counting changes.")
    #### UNCOMMENT TO SAVE A LOT OF DATA 
    ### df.to_csv(output_directory + '/all_entries_with_changed_flag.csv', index=False)
    ### print("Done saving all data with 'changed' flag...")
    if USE_DASK_DATAFRAME:
        return pd.DataFrame({'change_count': groups['changed'].sum().compute()}).reset_index()
    return pd.DataFrame({'change_count': groups['changed'].sum()}).reset_index()


def print_statistics(change_counts_df):
    # Calculate the total number of hosts
    total_hosts = change_counts_df['host_ce'].nunique()
    
    # Calculate the number of hosts with at least one parameter change
    hosts_with_changes = change_counts_df.groupby('host_ce')['change_count'].sum().reset_index()
    total_hosts_with_changes = hosts_with_changes[hosts_with_changes['change_count'] > 0].shape[0]
    
    total_hosts
    print(f"Total number of hosts: {total_hosts}")
    print(f"Number of hosts with at least one parameter change: {total_hosts_with_changes}")
    
    # Calculate the top most frequently changing parameters
    total_changes_per_param = change_counts_df.groupby('param_name')['change_count'].sum().sort_values(ascending=False)
    print("\nTop most frequently changing parameters:")
    print(total_changes_per_param.head(200))
    
    return total_hosts, total_hosts_with_changes

def generate_test_data():
    entries = []
    # Create entries for three hosts with known changes
    for day in range(3):
        timestamp = int(datetime(2024, 1, 1).timestamp()) + day * 86400  # Increment by one day
        for host_id in range(1, 4):
            entry = {
                'hostname': f'host{host_id}.example.com',
                'ce_name': 'ExampleCE',
                'last_updated': timestamp,
                'test_results_json': {
                    'test1': {'EXITCODE': 0, 'EXECUTION_TIME': 10, 'PARAM1': f'value_{host_id}_{day%2}'},  # Changes every day
                    'test2': {'EXITCODE': 0, 'EXECUTION_TIME': 20, 'PARAM2': f'value_{day}'},  # Changes every day
                }
            }
            entries.extend(flatten_entry(entry, timestamp))
    
    return entries

"""
def plot_param_host_count(df):
    # Count non-"N/A" entries for each parameter
    # param_counts = df[df['param_value'] != "N/A"].groupby('param_name')['host_ce'].nunique()
    param_counts = df.groupby('param_name')['host_ce'].nunique()

    # Plot histogram
    plt.figure(figsize=(45, 10))
    param_counts.plot(kind='bar')
    plt.title('Count of Hosts for Each Configuration Parameter')
    plt.xlabel('Configuration Parameters')
    plt.ylabel('Count of Hosts')
    plt.show()

    # return the host counts converted to DataFrame
    return dd.DataFrame({'host_count': param_counts}).reset_index()
    """

def read_entry(filename):
    
    entries = []

    timestamp = int(filename.split('-')[2].split('.')[0])
    with open(os.path.join(data_directory, filename), 'r') as file:
        print('processing file', filename, '...')
        for line in file:
            entry = json.loads(line.strip())
            entries.extend(flatten_entry(entry, timestamp))
        
    return entries

@count_time("main")
def main(test_run=False):
    global data_directory
    global output_directory
    if test_run:
        # Generate test data
        test_entries = generate_test_data()
        df = pd.DataFrame(test_entries)
        df = df.sort_values(by=['host_ce', 'param_name', 'date'])
        change_counts_df = compute_changes_across_entries(df)
        #change_counts_df.to_csv(output_directory + '/change_counts_df.csv', index=False)
        total_hosts, hosts_with_changes = print_statistics(change_counts_df)
        
        # Known outcome: All three hosts should have changes
        assert hosts_with_changes == 3, f"Expected 3 hosts with changes, but got {hosts_with_changes}"
        assert total_hosts == 3, f"Expected 3 total hosts, but got {total_hosts}"
        print("Test run passed successfully!")
    else:
        # Normal run
        all_entries = []
        futures = client.map(read_entry, [f for f in os.listdir(data_directory) if f.endswith('.out')][:8])
        wait(futures)
        accepted = [future for future in futures if future.status == "finished"]
        print(f'accepted {len(accepted)} / {len(futures)}')
        combined_entries = client.gather(accepted)
        all_entries = [entry for entries in combined_entries for entry in entries]
        
        """
        dd.set_option('display.max_colwidth', None)
        dd.options.display.max_rows = 4000
        dd.options.display.max_seq_items = 2000
        """
        pdf = pd.DataFrame(all_entries)
        df = None

        if USE_DASK_DATAFRAME:
            df = dd.from_pandas(pdf, npartitions = 10)
            df = df.sort_values('date')
            df = df.sort_values('param_name')
            df = df.sort_values('host_ce')
        else:
            df = pdf.sort_values(by=['host_ce', 'param_name', 'date'])

        print(df.shape)
        #### UNCOMMENT TO SAVE ALL DATA 
        ### df.to_csv('all_entries.csv', index=False)
        change_counts_df = compute_changes_across_entries(df)
        #### UNCOMMENT TO SAVE DATA 
        ### change_counts_df.to_csv(output_directory + '/change_counts.csv', index=False)
        print_statistics(change_counts_df)
        print("Change counts saved to change_counts.csv")
        
        # Plot the chart showing host count for every parameter
        # hostparam_df = plot_param_host_count(df)
        # hostparam_df.to_csv('host_param_counts.csv', index=False)


if __name__ == "__main__":

    start_time = time.time()

    # cluster = LocalCluster(n_workers = 16,
    #                       memory_target_fraction = 0.9,
    #                       memory_limit='4GB')

    # client = cluster.get_client()
    # client = Client(scheduler_file="/net/tscratch/people/plgmszewc/dask_tmp/scheduler.json")
    # initialize()
    client = Client()

    end_time = time.time()

    print(f'time to set up cluster {end_time - start_time}')

    # Run the main function with test_run=True to run the test, or test_run=False for normal execution
    main(test_run=False)

    client.close()