import argparse
import os
import shutil
import json
import pandas as pd
from glob import glob
from collections import defaultdict
from pandas import json_normalize
import matplotlib.pyplot as plt
import seaborn as sns
from plot_functions import get_rows_by_ips, create_ip_plot, print_hello_world, get_rows_by_arn, create_arn_plot
from plot_functions import get_rows_by_bucket, create_bucket_event_plot, search_and_plot_anonymous_events
from basic_stats import print_sorted_dict, tally_ips, tally_user_arns, tally_buckets
from query import print_rows_with_separator

parser = argparse.ArgumentParser(description='AWS Log Analysis Tool')
parser.add_argument('--mode', choices=['read', 'analyze', 'stats','query','clean'], help='Mode of operation')
parser.add_argument('--dir',  dest = 'dir', help='Directory to start looking from')
parser.add_argument('--analyze', choices=['IPs','arn','bucket','anon','else'], help='If Mode of operation is analyze')
parser.add_argument('--IPs',  dest = 'IPs', nargs='+', help='If youre analyzing IPs, need to pick some IPs')
parser.add_argument('--arn',  dest = 'arn', help='If youre analyzing an arn, need to pick a arn')
parser.add_argument('--bucket',  dest = 'bucket', help='If youre analyzing an bucket, need to pick a bucket')
parser.add_argument('--query', choices=['string'], help='If Mode of operation is query')
parser.add_argument('--strings',  dest = 'strings', nargs='+', help='If youre analyzing a strings, need to pick a eventName and string')


args = parser.parse_args()

# Create a seperate df for each event type, too much sparse data if all events in one frame
def create_dataframes_by_event_name(directory):
    records_by_event_name = defaultdict(list)

    # Construct the glob pattern to find all '.json' files
    pattern = os.path.join(directory, '**/*.json')  # This will match all .json files in the directory and subdirectories

    # Recursive glob to find all '.json' files in the directory
    for file_path in glob(pattern, recursive=True):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                # Assuming the JSON structure is like CloudTrail with 'Records' key
                records = data.get('Records', [])
                for record in records:
                    # Group records by 'eventName'
                    event_name = record.get('eventName')
                    records_by_event_name[event_name].append(record)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON file {file_path}: {e}")
        except Exception as e:
            print(f"An error occurred with file {file_path}: {e}")

    dataframes = {}
    # Create a dataframe for each event name
    for event_name, records in records_by_event_name.items():
        df = pd.DataFrame(records)
        dataframes[event_name] = df

    return dataframes

# Some columns didnt get djsoned all the way

def normalize_and_concat(df, column_name, prefix):
    try:
        # Check if the column contains lists with a single dictionary
        if df[column_name].apply(lambda x: isinstance(x, list)).all():
            df[column_name] = df[column_name].apply(lambda x: x[0] if len(x) == 1 else x)
        
        # Normalize the column
        normalized_df = json_normalize(df[column_name], sep='_')
        normalized_df.columns = [f'{prefix}{col}' for col in normalized_df.columns]
        
        # Concatenate the normalized data with the original dataframe
        return pd.concat([df.drop(columns=[column_name]), normalized_df], axis=1)
    except Exception as e:
        print(f"Error normalizing {column_name}: {e}")
        # Return the original DataFrame if normalization fails
        return df

def fix_columns(dfs):
    for event_name, df in dfs.items():
        # Apply normalization to 'userIdentity' column
        if 'userIdentity' in df.columns:
            df = normalize_and_concat(df, 'userIdentity', 'userIdentity_')

        # Apply normalization to 'requestParameters' column
        if 'requestParameters' in df.columns:
            df = normalize_and_concat(df, 'requestParameters', 'requestParameters_')

        # Apply normalization to 'resources' column
        if 'resources' in df.columns:
            df = normalize_and_concat(df, 'resources', 'resources_')

        # Update the original dictionary
        dfs[event_name] = df

    return dfs



# Inside analyze.py

def print_metadata():
    with open('metadata.txt', 'w') as metafile:  # Open a file called metadata.txt in write mode
        cdir = os.getcwd()
        #print(cdir)
        print('Printing Metadata to metadata.txt')
        for file in os.listdir(cdir):
            if file.endswith('.pkl'):
                df_path = os.path.join(cdir, file)
                df = pd.read_pickle(file)
                metafile.write(f"DataFrame from {file}:\n")
                metafile.write(f"Shape: {df.shape}\n")
                #metafile.write(f"Columns: {df.columns}\n")
                #metafile.write(str(df.head()) + "\n\n")  # Convert DataFrame to string before writing



# Main function to orchestrate the logic
def main():

    if args.mode == 'read':
        # Read and process logs, then save DataFrames
        dfs = read_and_process_logs()
        # Create directory to save dfs to
        save_dir = 'saved_dfs'
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        #Save the DataFrames to files
        print('Saving Pickles')
        for name, df in dfs.items():
            df.to_pickle(os.path.join(save_dir, f'{name}.pkl'))  # or df.to_csv for CSV files
        # Create dataframe metadata
        print_metadata()

    elif args.mode == 'stats':
        # Check if DataFrame files exist and load them
        dfs = {}
        save_dir = 'saved_dfs'
        for file in os.listdir(save_dir):
            if file.endswith('.pkl'):
                df_name = file.split('.')[0]
                file_path = os.path.join(save_dir, file)
                dfs[df_name] = pd.read_pickle(file_path)

        # Perform stats on the loaded DataFrames
        basic_stats(dfs)


    elif args.mode == 'analyze':
        # Check if DataFrame files exist and load them
        dfs = {}
        save_dir = 'saved_dfs'
        for file in os.listdir(save_dir):
            if file.endswith('.pkl'):
                df_name = file.split('.')[0]
                file_path = os.path.join(save_dir, file)
                dfs[df_name] = pd.read_pickle(file_path)

        # Perform analysis on the loaded DataFrames
        analyze_data(dfs)

    elif args.mode == 'query':
        query_data()



    elif args.mode == 'clean':
        # removes everything we made
        files_and_dirs_to_cleanup = ['metadata.txt', 'saved_dfs', '__pycache__']
        clean(files_and_dirs_to_cleanup)



# Function to read and process logs (to be implemented)
def read_and_process_logs():
    # Read log files and create DataFrames
    # Return a dictionary of DataFrames
    print('Importing and Parsing')
    dfs = create_dataframes_by_event_name(args.dir)
    print('fixing columns')
    fix_columns(dfs)
    fix_columns(dfs)
    return dfs

# Function to do basic stats on the data
def basic_stats(dfs):
    # Perform analysis on DataFrames
    ip_counts = tally_ips(dfs)
    print_sorted_dict(ip_counts,'Ip Counts')
    user_arn_counts = tally_user_arns(dfs)
    print_sorted_dict(user_arn_counts, "ARNs")
    bucket_counts = tally_buckets(dfs)
    print_sorted_dict(bucket_counts, "Buckets")




# Function to analyze the data (to be implemented)
def analyze_data(dfs):
   if args.analyze == 'IPs':
        ips_to_search = args.IPs
        print('Searching IPs '+ str(ips_to_search))
        resulting_df = get_rows_by_ips(dfs, ips_to_search)
        create_ip_plot(resulting_df)
   elif args.analyze == 'arn':
        arn_to_search = args.arn
        print('Slicing '+ arn_to_search)
        resulting_df = get_rows_by_arn(dfs, arn_to_search)
        create_arn_plot(resulting_df, arn_to_search)
   elif args.analyze == 'bucket':
        bucket_to_search = args.bucket
        print('Slicing '+ bucket_to_search)
        resulting_df = get_rows_by_bucket(dfs, bucket_to_search)
        create_bucket_event_plot(resulting_df, bucket_to_search)
   elif args.analyze == 'anon':
        search_and_plot_anonymous_events()

   else:
       # Perform analysis on DataFrames
       print_hello_world()

def query_data():
    if args.query == 'string':
        print_rows_with_separator(args.strings[0],args.strings[1])
    


def clean(files_and_dirs):
    for item in files_and_dirs:
        if os.path.isfile(item):
            os.remove(item)
        elif os.path.isdir(item):
            shutil.rmtree(item)


if __name__ == '__main__':
    main()
