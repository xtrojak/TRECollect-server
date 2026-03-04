from dotenv import load_dotenv
import argparse
import datetime
from zoneinfo import ZoneInfo

from APIs.google_spreadsheets import GoogleAPI
from APIs.owncloud import OwnCloudAPI
from processing.utils import get_last_run_timestamp


def main():
    load_dotenv('CONFIG.env')

    google_api = GoogleAPI()
    owncloud_api = OwnCloudAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    last_run_timestamp = get_last_run_timestamp()


    subfolders = owncloud_api.get_new_folders(last_run_timestamp)
    for subfolder in subfolders:
        print(subfolder)

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Process new logsheet submissions')

    args_parser._action_groups.pop()
    optional = args_parser.add_argument_group('optional arguments')
    
    args = args_parser.parse_args()
    main()
