import pandas as pd
import os
from pathlib import Path

def print_rows_with_separator(event_name, search_string, directory='./saved_dfs', output_file='query_output.txt'):
    # Determine which pickle files to process
    if event_name.upper() == 'ALL':
        pickle_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.pkl')]
    else:
        pickle_file = os.path.join(directory, f"{event_name}.pkl")
        if not os.path.exists(pickle_file):
            with open(output_file, 'w') as file:
                file.write(f"File {pickle_file} does not exist.\n")
            return
        pickle_files = [pickle_file]

    # ANSI escape code for red color
    start_red = "\033[91m"
    end_red = "\033[0m"
    
    # Open the output file to write the results
    with open(output_file, 'w') as file:
        for pickle_file in pickle_files:
            # Check if the file exists before proceeding
            if not os.path.exists(pickle_file):
                file.write(f"File {pickle_file} does not exist.\n")
                continue
            
            # Load the DataFrame from the pickle file
            df = pd.read_pickle(pickle_file)
            
            # Apply a mask to find rows containing the search string
            matches = df.applymap(lambda x: search_string in str(x)).any(axis=1)
            
            # Iterate over each matching row
            for i, row in df[matches].iterrows():
                # Write each column-value pair in the row to the file
                for col, val in row.items():
                    # Highlight the search string in red
                    val_str = str(val)
                    if search_string in val_str:
                        val_str = val_str.replace(search_string, f"{start_red}{search_string}{end_red}")
                    file.write(f"{col}: {val_str}\n")
                # Write a separator after each row
                file.write('---\n')

