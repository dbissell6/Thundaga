import pandas as pd
import os

def print_rows_with_separator(event_name, search_string, directory='./saved_dfs'):
    # Construct the full file path from the event name
    pickle_file = os.path.join(directory, f"{event_name}.pkl")

    # Check if the file exists before proceeding
    if not os.path.exists(pickle_file):
        print(f"File {pickle_file} does not exist.")
        return

    # Load the DataFrame from the pickle file
    df = pd.read_pickle(pickle_file)
    
    # Apply a mask to find rows containing the search string
    matches = df.applymap(lambda x: search_string in str(x)).any(axis=1)
    
    # ANSI escape code for red color
    start_red = "\033[91m"
    end_red = "\033[0m"

    # Iterate over each matching row
    for i, row in df[matches].iterrows():
        # Print each column-value pair in the row
        for col, val in row.items():
            val_str = str(val)
            # Highlight the search string in red
            if search_string in val_str:
                val_str = val_str.replace(search_string, f"{start_red}{search_string}{end_red}")
            print(f"{col}: {val_str}")
        # Print separator after each row, not after each value
        print('---')
