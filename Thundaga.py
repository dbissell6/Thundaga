import argparse
from data_processing import process_data
from plot import run_dash_app, run_wlogs_app

def main():
    parser = argparse.ArgumentParser(description='AWS + EVTX  Log Visualization Tool')
    parser.add_argument('--dir',  dest = 'dir', required=True, help='Directory to start looking from for aws logs, or the actual sigma.csv')
    parser.add_argument('--evid', choices=['AWS','Wlogs'], help='AWS cloudlogs in JSON or Chainsaw sigma output of evtx logs')
    args = parser.parse_args()

    if args.evid == 'AWS':
        # Process the JSON files to create the consolidated DataFrame
        consolidated_logs_df = process_data(args.dir)
        run_dash_app(consolidated_logs_df)
    elif args.evid == 'Wlogs':
        run_wlogs_app(args.dir)

if __name__ == '__main__':
    main()
