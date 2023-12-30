import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os



# This is a dummy function for demonstration.
def print_hello_world():
    print("Hello, world!")


def get_rows_by_ips(dfs, ips_of_interest):
    # Initialize an empty DataFrame to hold all matching rows
    df_final = pd.DataFrame()

    # Iterate over each dataframe and its name in the dictionary
    for df_name, df in dfs.items():
        # Check if 'sourceIPAddress' column exists in the dataframe
        if 'sourceIPAddress' in df.columns:
            # Filter rows where the sourceIPAddress is in the list of IPs of interest
            matching_rows = df[df['sourceIPAddress'].isin(ips_of_interest)].copy()
            # If there are matching rows, concatenate them to the final dataframe
            if not matching_rows.empty:
                # Add a column to indicate the DataFrame source (event name)
                matching_rows.loc[:,'EventName'] = df_name
                df_final = pd.concat([df_final, matching_rows], ignore_index=True)

    return df_final

def create_ip_plot(resulting_df):

    unique_event_names = resulting_df['eventName'].unique()
    markers = ['o', 's', '^', 'P', '*', 'D', 'X', '<', '>']  # Add more markers if needed
    colors = plt.cm.tab10(np.linspace(0, 1, len(unique_event_names)))
    
    # Map each event name to a marker and color
    event_marker_color_map = {
        event_name: {'marker': markers[i % len(markers)], 'color': colors[i]} 
        for i, event_name in enumerate(unique_event_names)
    }
    
    # Plot the events
    plt.figure(figsize=(15, 10))
    
    # Create a new column in the DataFrame that maps the event names to y-axis levels
    resulting_df['y_level'] = resulting_df['sourceIPAddress'].astype("category").cat.codes
    
    # Plot each event with its assigned marker and color
    for event_name, group in resulting_df.groupby('eventName'):
        plt.scatter(
            group['eventTime'], 
            group['y_level'], 
            alpha=0.7, 
            marker=event_marker_color_map[event_name]['marker'],
            color=event_marker_color_map[event_name]['color'],
            label=event_name
        )
    
    # Adjust the y-axis to show IP addresses instead of numeric levels
    plt.yticks(resulting_df['y_level'].unique(), resulting_df['sourceIPAddress'].unique())
    
    # Improve plot aesthetics
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xlabel('Event Time')
    plt.ylabel('IP Address')
    plt.title('Event Activities Over Time by Event Name')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Show the plot
    plt.show()



### Next functions do something similar for arns

def get_rows_by_arn(dfs, arn_of_interest):
    df_final = pd.DataFrame()

    for df_name, df in dfs.items():
        if 'userIdentity_arn' in df.columns:
            # Filter rows where the userIdentity_arn matches the ARN of interest
            matching_rows = df[df['userIdentity_arn'] == arn_of_interest].copy()
            if not matching_rows.empty:
                matching_rows.loc[:, 'EventName'] = df_name
                df_final = pd.concat([df_final, matching_rows], ignore_index=True)

    return df_final

def create_arn_plot(resulting_df, arn_to_search):
    unique_event_names = resulting_df['eventName'].unique()
    markers = ['o', 's', '^', 'P', '*', 'D', 'X', '<', '>']
    colors = plt.cm.tab10(np.linspace(0, 1, len(unique_event_names)))

    event_marker_color_map = {event_name: {'marker': markers[i % len(markers)], 'color': colors[i]} for i, event_name in enumerate(unique_event_names)}

    plt.figure(figsize=(15, 10))
    resulting_df['y_level'] = resulting_df['sourceIPAddress'].astype("category").cat.codes

    for event_name, group in resulting_df.groupby('eventName'):
        plt.scatter(group['eventTime'], group['y_level'], alpha=0.7, marker=event_marker_color_map[event_name]['marker'], color=event_marker_color_map[event_name]['color'], label=event_name)

    plt.yticks(resulting_df['y_level'].unique(), resulting_df['sourceIPAddress'].unique())
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xlabel('Event Time')
    plt.ylabel('IP Address')
    plt.title(f'Event Activities Over Time for ARN: {arn_to_search}')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

### Same thing for a bucket

def get_rows_by_bucket(dfs, bucket_of_interest, exclude_ips=None):
    exclude_ips = exclude_ips or []  # List of IPs to exclude
    df_final = pd.DataFrame()  # Initialize an empty DataFrame to hold all matching rows

    # Iterate over each dataframe and its name in the dictionary
    for df_name, df in dfs.items():
        if 'requestParameters_bucketName' in df.columns:
            # Filter rows where the bucket name is mentioned in 'requestParameters'
            matching_rows = df[df['requestParameters_bucketName'].astype(str).str.contains(bucket_of_interest)].copy()
            # Exclude rows with IPs that are in the exclude_ips list
            matching_rows = matching_rows[~matching_rows['sourceIPAddress'].isin(exclude_ips)]
            if not matching_rows.empty:
                matching_rows.loc[:, 'EventName'] = df_name  # Add a column to indicate the DataFrame source (event name)
                df_final = pd.concat([df_final, matching_rows], ignore_index=True)  # Concatenate them to the final dataframe

    return df_final


def create_bucket_event_plot(resulting_df, bucket_of_interest):
    # Extract unique event names and source IP addresses
    unique_event_names = resulting_df['EventName'].unique()
    unique_ips = resulting_df['sourceIPAddress'].unique()

    # Assign a unique marker and color to each event name
    markers = ['o', 's', '^', 'P', '*', 'D', 'X', '<', '>']  # Add more markers if needed
    colors = plt.cm.tab10(np.linspace(0, 1, len(unique_event_names)))

    event_marker_color_map = {
        event_name: {'marker': markers[i % len(markers)], 'color': colors[i]} 
        for i, event_name in enumerate(unique_event_names)
    }

    # Plot the events
    plt.figure(figsize=(15, 10))

    # Create a new column in the DataFrame that maps IP addresses to y-axis levels
    resulting_df['y_level'] = resulting_df['sourceIPAddress'].astype("category").cat.codes

    # Plot each event with its assigned marker and color
    for event_name, group in resulting_df.groupby('EventName'):
        plt.scatter(
            group['eventTime'], 
            group['y_level'], 
            alpha=0.7, 
            marker=event_marker_color_map[event_name]['marker'],
            color=event_marker_color_map[event_name]['color'],
            label=event_name
        )

    # Adjust the y-axis to show IP addresses instead of numeric levels
    plt.yticks(resulting_df['y_level'].unique(), unique_ips)
    
    # Improve plot aesthetics
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xlabel('Event Time')
    plt.ylabel('IP Address')
    plt.title(f'Event Activities Over Time for Bucket {bucket_of_interest}')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Show the plot
    plt.show()


### Create plots for anons

def search_and_plot_anonymous_events(exclude_ips=None,column_name='eventName'):
    exclude_ips = exclude_ips or []
    anonymous_entries = {}
    path = './saved_dfs'
    
    # List all .pkl files in the directory
    pkl_files = [file for file in os.listdir(path) if file.endswith('.pkl')]
    df_final = pd.DataFrame()

    # Loop through the .pkl files
    for file in pkl_files:
        # Read the DataFrame
        df = pd.read_pickle(os.path.join(path, file))
        
        # Check each row for 'anonymous'
        for index, row in df.iterrows():
            if 'anonymous' in row.astype(str).values:
                # Record the event name and IP if 'anonymous' is found
                event_name = row.get(column_name, 'No-Event-Name-Column')
                ip_address = row.get('sourceIPAddress', 'No-IP-Address-Column')

                if ip_address in exclude_ips:
                    continue
                # Create a new DataFrame from the anonymous rows
                anonymous_row = df.loc[[index]]
                anonymous_row['EventName'] = event_name
                df_final = pd.concat([df_final, anonymous_row], ignore_index=True)


    # Proceed with plotting if we found any anonymous events
    if not df_final.empty:
        unique_event_names = df_final['EventName'].unique()
        markers = ['o', 's', '^', 'P', '*', 'D', 'X', '<', '>']
        colors = plt.cm.tab10(np.linspace(0, 1, len(unique_event_names)))

        event_marker_color_map = {
            event_name: {
                'marker': markers[i % len(markers)],
                'color': colors[i]
            } for i, event_name in enumerate(unique_event_names)
        }

        plt.figure(figsize=(15, 10))
        df_final['y_level'] = df_final['sourceIPAddress'].astype("category").cat.codes

        for event_name, group in df_final.groupby('EventName'):
            plt.scatter(
                pd.to_datetime(group['eventTime']),
                group['y_level'],
                alpha=0.7,
                marker=event_marker_color_map[event_name]['marker'],
                color=event_marker_color_map[event_name]['color'],
                label=event_name
            )

        plt.yticks(df_final['y_level'].unique(), df_final['sourceIPAddress'].unique())
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xlabel('Event Time')
        plt.ylabel('IP Address')
        plt.title('Event Activities Over Time for Anonymous Events')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    return df_final

