import pandas as pd
import matplotlib.pyplot as plt
import numpy as np




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
