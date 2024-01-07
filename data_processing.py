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
