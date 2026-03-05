from dotenv import load_dotenv
import argparse
import datetime
from zoneinfo import ZoneInfo

from APIs.google_spreadsheets import GoogleAPI
from APIs.owncloud import OwnCloudAPI
from processing.utils import get_last_data_timestamp, get_last_config_timestamp, load_config_versions


def main():
    load_dotenv('CONFIG.env')

    google_api = GoogleAPI()
    owncloud_api = OwnCloudAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    last_run_timestamp = get_last_data_timestamp()
    last_config_timestamp = get_last_config_timestamp()

    # download new submitted sites
    subfolders = owncloud_api.get_new_folders(last_run_timestamp)

    # get configs
    config_files = owncloud_api.get_new_config_files("logsheets", "downloaded_configs", last_config_timestamp)

    # load configs
    configs = load_config_versions("downloaded_configs")
    
    # process the new sites
    # download files, delete them after processing
    # and push them to google sheets
    # and back them up to
    # and update the last run timestamp

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Process new logsheet submissions')

    args_parser._action_groups.pop()
    optional = args_parser.add_argument_group('optional arguments')
    
    args = args_parser.parse_args()
    main()
