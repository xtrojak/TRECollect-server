import os
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from APIs.google_spreadsheets import GoogleAPI
from processing.utils import get_last_backup_timestamp, save_last_backup_timestamp

def merge_and_backup_submissions():
    """Main function to merge and backup submissions based on checkbox status"""

    load_dotenv('CONFIG.env')
    
    latest_submissions_source_sheet_id = os.environ.get('LSI_SHEET_LATEST_SUBMISSIONS_ID')
    latest_submissions_backup_sheet_id = os.environ.get('LSI_SHEET_LATEST_SUBMISSIONS_BACKUP_ID')
    all_submissions_source_sheet_id = os.environ.get('LSI_SHEET_ALL_SUBMISSIONS_ID')
    all_submissions_backup_sheet_id = os.environ.get('LSI_SHEET_ALL_SUBMISSIONS_BACKUP_ID')
    
    google_api = GoogleAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))

    print(f">>> Backup at {now}")

    # 1) Backup "latest submissions" sheet if it changed since last check.
    last_backup_timestamp = get_last_backup_timestamp()

    if google_api.detect_changes(latest_submissions_source_sheet_id, last_backup_timestamp):
        print(f">>> Changes detected in latest submissions since {last_backup_timestamp}")
        google_api.backup_spreadsheet(latest_submissions_source_sheet_id, latest_submissions_backup_sheet_id)
    else:
        print(f">>> No changes detected in latest submissions since {last_backup_timestamp}")

    # 2) If Review checkbox is ticked, merge latest -> all, then refresh backups and clear latest.
    if google_api.is_checkbox_checked(latest_submissions_source_sheet_id, "Review"):
        print(">>> Review checkbox is checked. Starting merge process...")

        source_worksheets = google_api.get_all_worksheets(latest_submissions_source_sheet_id)
        for worksheet_name in source_worksheets:
            if worksheet_name == "Review":
                continue

            source_data = google_api.read_table(latest_submissions_source_sheet_id, worksheet_name)
            if source_data.empty:
                continue

            row_dicts = source_data.to_dict(orient="records")
            google_api.add_rows(all_submissions_source_sheet_id, worksheet_name, row_dicts)
            google_api.clear_worksheet_data(latest_submissions_source_sheet_id, worksheet_name)

        # Untick checkbox after merge
        google_api.set_checkbox(latest_submissions_source_sheet_id, "Review", checked=False)

        print(f">>> Backup all submissions...")
        # Backup "all submissions" again after merge to capture merged state
        google_api.backup_spreadsheet(all_submissions_source_sheet_id, all_submissions_backup_sheet_id)

        print(">>> Merge process completed successfully!")
    else:
        print(">>> Review checkbox is not checked. No merge performed.")

    save_last_backup_timestamp(now)

if __name__ == "__main__":
    merge_and_backup_submissions()
