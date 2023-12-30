import pandas as pd
import os

def print_sorted_dict(d, title):
    print(title)
    # Sort the dictionary by value in descending order and print the items
    for key, value in sorted(d.items(), key=lambda item: item[1], reverse=True):
        print(f"{key}: {value}")

    print('')


# Function to tally IP addresses across multiple dataframes

def tally_ips(dfs):
    ip_tally = {}
    for event_name, df in dfs.items():
        if 'sourceIPAddress' in df.columns:
            ips = df['sourceIPAddress']
            for ip in ips:
                if ip in ip_tally:
                    ip_tally[ip] += 1
                else:
                    ip_tally[ip] = 1
    return ip_tally


## Tally logs based on the username associated
# Function to tally userIdentity_arn across multiple dataframes
def tally_user_arns(dataframes):
    user_arn_tally = {}
    for event_name, df in dataframes.items():
        if 'userIdentity_arn' in df.columns:
            user_arns = df['userIdentity_arn']

            for user_arn in user_arns:
                if user_arn in user_arn_tally:
                    user_arn_tally[user_arn] += 1
                elif pd.isna(user_arn) != True:
                    user_arn_tally[user_arn] = 1
    
    return user_arn_tally


## Tally buckets used
def tally_buckets(dfs):
    bucket_tally = {}
    for event_name, df in dfs.items():
        if 'requestParameters_bucketName' in df.columns:
            bucket_names = df['requestParameters_bucketName']

            for bucket_name in bucket_names:
                if pd.notna(bucket_name):  # Check if bucket name is not NaN
                    if bucket_name in bucket_tally:
                        bucket_tally[bucket_name] += 1
                    else:
                        bucket_tally[bucket_name] = 1

    return bucket_tally






