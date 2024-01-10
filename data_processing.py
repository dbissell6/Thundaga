import os
import json
import pandas as pd
from collections import defaultdict
from glob import glob

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
                    # Flatten nested JSON objects here if necessary
                    flat_record = flatten_json(record)
                    records_by_event_name[event_name].append(flat_record)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON file {file_path}: {e}")
        except Exception as e:
            print(f"An error occurred with file {file_path}: {e}")
    dataframes = {}
    # Create a dataframe for each event name
    for event_name, records in records_by_event_name.items():
        df = pd.json_normalize(records)
        dataframes[event_name] = df

    return dataframes

def flatten_json(nested_json):
    """
    Flatten a nested json file
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, f"{name}{i}_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out



def consolidate_logs(dataframes_dict, columns):
    # List to hold data from all frames
    all_data = []
    
    # Iterate through each dataframe in the dictionary
    for event_name, df in dataframes_dict.items():
        if set(columns).issubset(df.columns):
            # Copy the event name into a new column to preserve it
            df['eventName'] = event_name
            # Select the relevant columns and add to the list
            all_data.append(df[columns])
        else:
            print(f"Warning: Not all columns found in DataFrame for event {event_name}")

    # Concatenate all dataframes into one
    consolidated_df = pd.concat(all_data, ignore_index=True)
    
    # Convert eventTime to datetime if it's not already in that format
    if consolidated_df['eventTime'].dtype == object:
        consolidated_df['eventTime'] = pd.to_datetime(consolidated_df['eventTime'])

    return consolidated_df

def process_data(directory):
    # Define the columns you want to extract
    required_columns = ['eventTime', 'eventName', 'sourceIPAddress', 'eventID']

    # Create dataframes by event name
    dataframes_by_event = create_dataframes_by_event_name(directory)

    # Consolidate logs into a single dataframe
    consolidated_logs_df = consolidate_logs(dataframes_by_event, required_columns)
    return consolidated_logs_df


def query_and_print_logs(dataframes, search_term):
    # Initialize a list to collect DataFrames
    filtered_dataframes = []

    for key, dataframe in dataframes.items():
        query_mask = dataframe.astype(str).apply(
            lambda row: row.str.contains(search_term, case=False, na=False).any(), axis=1
        )
        filtered_dataframe = dataframe[query_mask]
        # Only append non-empty DataFrames
        if not filtered_dataframe.empty:
            filtered_dataframes.append(filtered_dataframe)

    # Use concat to combine the DataFrames in the list
    if filtered_dataframes:  # Ensure the list is not empty
        aggregated_df = pd.concat(filtered_dataframes, ignore_index=True)
        aggregated_df = aggregated_df.sort_values(by='eventTime')
        write_readable_logs_to_file(aggregated_df)  # Assuming print_readable_logs is defined elsewhere
    else:
        print("No records found with the search term.")

def write_readable_logs_to_file(df):
    with open('output.txt', 'w') as file:
        for _, row in df.iterrows():
            for col, value in row.items():
                if pd.notna(value):
                    file.write(f"{col}: {value}\n")
            file.write('-' * 40 + '\n')  # Separator line for readability



def create_stats_file(df):
    df.fillna('Missing', inplace=True)
    print(df.columns)
    with open('counts.txt', 'w') as f:
        f.write('IP Counts:\n')
        df['sourceIPAddress'].value_counts().reset_index().to_csv(f, sep='\t', index=False, header=False)
        f.write('\nAccount ID Counts:\n')
        df['userIdentity_accountId'].value_counts().reset_index().to_csv(f, sep='\t', index=False, header=False)
        f.write('\nARN Counts:\n')
        df['userIdentity_arn'].value_counts().reset_index().to_csv(f, sep='\t', index=False, header=False)
        f.write('\nUser Agent Counts:\n')
        df['userAgent'].value_counts().reset_index().to_csv(f, sep='\t', index=False, header=False)
        f.write('\nBucket Name Counts:\n')
        df['requestParameters_bucketName'].value_counts().reset_index().to_csv(f, sep='\t', index=False, header=False)

