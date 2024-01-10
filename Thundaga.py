import argparse
from data_processing import consolidate_logs, create_dataframes_by_event_name, process_data, query_and_print_logs, create_stats_file
from plot import run_dash_app, run_wlogs_app

def main():
    parser = argparse.ArgumentParser(description='AWS + EVTX  Log Visualization Tool')
    parser.add_argument('--dir',  dest = 'dir', required=True, help='Directory to start looking from for aws logs, or the actual sigma.csv')
    parser.add_argument('--evid', choices=['AWS','AWSQ','AWSstats','Wlogs'], help='AWS cloudlogs in JSON or Chainsaw sigma output of evtx logs')
    parser.add_argument('--query',  dest = 'query', help='Term to search, output to output.txt')
    args = parser.parse_args()

    if args.evid == 'AWS':
        # Process the JSON files to create the consolidated DataFrame
        consolidated_logs_df = process_data(args.dir)
        #print(consolidated_logs_df)
        run_dash_app(consolidated_logs_df)
    elif args.evid == 'AWSstats':
        df = create_dataframes_by_event_name(args.dir)
        consolidated_logs_df = consolidate_logs(df,['eventTime','sourceIPAddress','userIdentity_accountId','userIdentity_arn','userAgent','requestParameters_bucketName'])
        create_stats_file(consolidated_logs_df)
    elif args.evid == 'AWSQ':
        consolidated_logs_df = create_dataframes_by_event_name(args.dir)
        query_and_print_logs(consolidated_logs_df, args.query)

    elif args.evid == 'Wlogs':
        run_wlogs_app(args.dir)

if __name__ == '__main__':
    main()

