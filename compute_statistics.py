from dotenv import load_dotenv
import os
import datetime
from zoneinfo import ZoneInfo

from processing.utils import load_config_versions
from curation.statistics import compute_and_save_statistics
from APIs.google_spreadsheets import GoogleAPI
from processing.utils import get_last_statistics_timestamp, save_last_statistics_timestamp


def detect_changes(google_api: GoogleAPI, lsi_target_sheet_id: str, last_statistics_timestamp: datetime.datetime):
    modified_time = google_api.get_modified_time(lsi_target_sheet_id)
    modified_time = datetime.datetime.fromisoformat(modified_time)
    return modified_time > last_statistics_timestamp


def main():
    load_dotenv('CONFIG.env')
    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    google_api = GoogleAPI()
    lsi_target_sheet_id = os.environ.get('LSI_SHEET_LATEST_SUBMISSIONS_ID')

    last_statistics_timestamp = get_last_statistics_timestamp()

    if detect_changes(google_api, lsi_target_sheet_id, last_statistics_timestamp):
        print(f'>>> Changes detected since {last_statistics_timestamp}')
        configs = load_config_versions("downloaded_configs")
        data = google_api.read_tables(lsi_target_sheet_id)
        compute_and_save_statistics(data, configs)
        save_last_statistics_timestamp(now)
    else:
        print(f'>>> No changes detected since {last_statistics_timestamp}')

if __name__ == '__main__':
    main()
