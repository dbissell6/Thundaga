import argparse
import os
import json
import pandas as pd
from glob import glob
from collections import defaultdict
from pandas import json_normalize
import matplotlib.pyplot as plt
import seaborn as sns
from plot_functions import get_rows_by_ips, create_ip_plot, print_hello_world, get_rows_by_arn, create_arn_plot
from plot_functions import get_rows_by_bucket, create_bucket_event_plot
from basic_stats import print_sorted_dict, tally_ips, tally_user_arns, tally_buckets

parser = argparse.ArgumentParser(description='AWS Log Analysis Tool')
parser.add_argument('--mode', choices=['read', 'analyze', 'stats'], help='Mode of operation: read or analyze')
parser.add_argument('--dir',  dest = 'dir', help='Directory to start looking from')
parser.add_argument('--analyze', choices=['IPs','arn','bucket','else'], help='If Mode of operation is analyze')
parser.add_argument('--IPs',  dest = 'IPs', nargs='+', help='If youre analyzing IPs, need to pick some IPs')
parser.add_argument('--arn',  dest = 'arn', help='If youre analyzing an arn, need to pick a arn')
parser.add_argument('--bucket',  dest = 'bucket', help='If youre analyzing an bucket, need to pick a bucket')


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
def fix_columns(dfs):
  for event_name, df in dfs.items():
        # Check if 'userIdentity' column exists in the dataframe
        if 'userIdentity' in df.columns:
            # Normalize the 'userIdentity' column
            userIdentity_normalized = json_normalize(df['userIdentity'], sep='_', record_prefix='userIdentity')


            prefix = 'userIdentity_'
            userIdentity_normalized = userIdentity_normalized.rename(columns={col: f'{prefix}{col}' for col in userIdentity_normalized.columns})

            # Concatenate the normalized userIdentity data with the original dataframe
            # Make sure to align the index to ensure the data matches up correctly
            dfs[event_name] = pd.concat([df.drop(columns=['userIdentity']), userIdentity_normalized], axis=1)


        if 'requestParameters' in df.columns:
            try:
                # Try to normalize the 'requestParameters' column
                requestParameters_normalized = json_normalize(df['requestParameters'], sep='_', record_prefix='requestParameters_')
                prefix = 'requestParameters_'
                requestParameters_normalized = requestParameters_normalized.rename(columns={col: f'{prefix}{col}' for col in requestParameters_normalized.columns})

                # Concatenate the normalized userIdentity data with the original dataframe
                # Rename the columns with prefix and concatenate with the original dataframe
                dfs[event_name] = pd.concat([df.drop(columns=['requestParameters']), requestParameters_normalized], axis=1)

            except (ValueError, AttributeError):
                # Try to extract from list if normalization fails
                try:
                    # Assuming that if it's a list, we take the first element
                    df['requestParameters'] = df['requestParameters'].apply(lambda x: x[0] if isinstance(x, list) and x else x)
                    # Try normalization again after extracting from list
                    requestParameters_normalized = json_normalize(df['requestParameters'], sep='_', record_prefix='requestParameters_')
                    dfs[event_name] = pd.concat([df.drop(columns=['requestParameters']), requestParameters_normalized], axis=1)

                except (ValueError, AttributeError, IndexError):
                    # Skip this dataframe if second normalization also fails
                    #print(f"Skipping normalization for 'requestParameters' in DataFrame: {event_name}")
                    pass


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

   else:
       # Perform analysis on DataFrames
       print_hello_world()



if __name__ == '__main__':
    main()
