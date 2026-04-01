import pandas as pd
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from typing import Optional, List
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleServiceAccountCredentials
from google.auth.transport.requests import Request as GoogleAuthRequest
import datetime
import requests

from APIs.utils import rate_limited_with_retry, clean_up_nulls, create_keyfile_dict


class GoogleAPI:
    def __init__(self):
        keyfile_dict = create_keyfile_dict()

        sheets_scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self._sheets_creds = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict, sheets_scopes)
        self.client = gspread.authorize(self._sheets_creds)

        drive_scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
        self._drive_meta_creds = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict, drive_scopes)

        weather_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self._weather_oauth_creds = GoogleServiceAccountCredentials.from_service_account_info(
            keyfile_dict,
            scopes=weather_scopes,
        )

    @rate_limited_with_retry()
    def get_modified_time(self, file_key):
        drive = build("drive", "v3", credentials=self._drive_meta_creds)

        meta = drive.files().get(fileId=file_key, fields="modifiedTime").execute()
        return meta['modifiedTime']

    @staticmethod
    def _sheet_value(val):
        """Convert a cell value to something JSON-serializable for the Sheets API."""
        if pd.isna(val):
            return ""
        if hasattr(val, "item"):  # numpy scalar
            return val.item()
        return val

    @rate_limited_with_retry()
    def access_sheet(self, file_key, worksheet, create_if_missing=True, rows=1000, cols=50):
        """Access remote Google worksheet, creating it if it does not exist.

        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab (title)
            create_if_missing (bool): if True, create a new worksheet with the given title when not found (default True)
            rows (int): number of rows when creating a new worksheet (default 1000)
            cols (int): number of columns when creating a new worksheet (default 26)

        Returns:
            sheet: Google sheet
        """
        spreadsheet = self.client.open_by_key(file_key)
        try:
            return spreadsheet.worksheet(worksheet)
        except WorksheetNotFound:
            if create_if_missing:
                return spreadsheet.add_worksheet(title=worksheet, rows=rows, cols=cols)
            raise

    @rate_limited_with_retry()
    def read_table(self, file_key, worksheet):
        """Read remote Google sheet as a Pandas dataframe

        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab

        Returns:
            dataframe: created Pandas dataframe
        """
        sheet = self.access_sheet(file_key, worksheet)
        data = sheet.get_all_values()
        header = data[0]
        df = pd.DataFrame(data[1:], columns=header)
        return df
    
    @rate_limited_with_retry()
    def overwrite_table(self, file_key, worksheet, df):
        """Write table to the Google sheet

        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab
            df (dataframe): target table as dataframe
        """
        spreadsheet = self.client.open_by_key(file_key)
        sheet = spreadsheet.worksheet(worksheet)
        # Ensure all cells are JSON-serializable (pd.NA / np.nan are not)
        header = [self._sheet_value(v) for v in df.columns]
        rows = [[self._sheet_value(v) for v in row] for row in df.values.tolist()]
        sheet.update([header] + rows)

    @rate_limited_with_retry()
    def add_rows(self, file_key, worksheet, row_dicts):
        """Add a list of rows to the Google sheet

        Also resolve any changes in the headers

        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab
            row_dicts (list): list of dict representation of logsheet contents
        """
        sheet = self.access_sheet(file_key, worksheet)
        # Get current header
        header = sheet.row_values(1)

        # Detect and add missing columns
        missing_cols = [key for key in row_dicts[0] if key not in header]
        if missing_cols:
            updated_header = header + missing_cols
            sheet.update('1:1', [updated_header])
            header = updated_header

        # Prepare row values in correct order
        values_to_append = []
        for row in row_dicts:
            row_values = [row.get(col, "") for col in header]
            values_to_append.append(clean_up_nulls(row_values))

        # Append rows
        sheet.append_rows(values_to_append)

    @rate_limited_with_retry()
    def get_header(self, file_key, worksheet):
        sheet = self.access_sheet(file_key, worksheet)
        return sheet.row_values(1)

    @rate_limited_with_retry()
    def is_checkbox_checked(self, file_key, worksheet, cell='A1'):
        """Check if a checkbox in the specified cell is checked
        
        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab
            cell (str): cell reference (default: 'A1')
            
        Returns:
            bool: True if checkbox is checked, False otherwise
        """
        sheet = self.access_sheet(file_key, worksheet)
        try:
            cell_value = sheet.acell(cell).value
            # Google Sheets checkboxes return "TRUE" or "FALSE" as strings
            return cell_value == "TRUE"
        except Exception as e:
            print(f"Error checking checkbox in {cell}: {e}")
            return False

    @rate_limited_with_retry()
    def set_checkbox(self, file_key, worksheet, cell='A1', checked=False):
        """Set a checkbox in the specified cell
        
        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab
            cell (str): cell reference (default: 'A1')
            checked (bool): whether to check or uncheck the checkbox
        """
        sheet = self.access_sheet(file_key, worksheet)
        value = True if checked else False
        sheet.update(cell, [[value]])

    @rate_limited_with_retry()
    def get_all_worksheets(self, file_key):
        """Get all worksheet names from a spreadsheet
        
        Args:
            file_key (str): identifier of Google sheet
            
        Returns:
            list: list of worksheet names
        """
        try:
            spreadsheet = self.client.open_by_key(file_key)
            return [ws.title for ws in spreadsheet.worksheets()]
        except Exception as e:
            print(f"Error getting worksheets: {e}")
            return []

    @rate_limited_with_retry()
    def read_tables(self, file_key, sheet_names: Optional[List[str]] = None):
        """Load specified worksheets in the spreadsheet as DataFrames.
        
        Args:
            file_key (str): identifier of Google sheet
            sheet_names (list): list of worksheet names to load
        Returns:
            dict: worksheet name -> pandas DataFrame
        """
        if not file_key:
            return {}
        if sheet_names:
            return {name: self.read_table(file_key, name) for name in sheet_names}
        else:
            sheet_names = self.get_all_worksheets(file_key)
            return {name: self.read_table(file_key, name) for name in sheet_names}

    @rate_limited_with_retry()
    def clear_worksheet_data(self, file_key, worksheet):
        """Clear all data rows from a worksheet, preserving the header row (and its formatting).
        
        Args:
            file_key (str): identifier of Google sheet
            worksheet (str): identifier of sheet tab
        """
        sheet = self.access_sheet(file_key, worksheet)
        last_row = sheet.row_count
        if last_row <= 1:
            return
        # Clear everything below the first row, leaving headers (and their formatting) intact.
        # This uses A1 notation "2:<last_row>" to wipe rows 2..N.
        sheet.batch_clear([f"2:{last_row}"])
        sheet.freeze(rows=1)

    @rate_limited_with_retry()
    def backup_spreadsheet(self, source_file_key: str, target_file_key: str) -> None:
        """
        Replace the contents of the target spreadsheet with an exact copy
        of all worksheets from the source spreadsheet.

        Whatever existed in the target spreadsheet before is discarded.

        This uses Google Sheets' native sheet copy operation so values and formatting
        are preserved.
        """
        source = self.client.open_by_key(source_file_key)
        target = self.client.open_by_key(target_file_key)

        source_sheets = source.worksheets()
        if not source_sheets:
            # If there's nothing to copy, just clear the target completely (leave a single empty sheet).
            existing = target.worksheets()
            if len(existing) == 0:
                target.add_worksheet(title="Sheet1", rows=100, cols=26)
                return
            # Keep one sheet, delete the rest, and clear its contents.
            keep = existing[0]
            for ws in existing[1:]:
                target.del_worksheet(ws)
            keep.clear()
            return

        # Ensure we can delete all existing sheets by creating a temporary one.
        tmp_title = "__tmp_backup__"
        try:
            tmp = target.worksheet(tmp_title)
        except WorksheetNotFound:
            tmp = target.add_worksheet(title=tmp_title, rows=10, cols=10)

        # Delete everything that existed in target.
        for ws in [w for w in target.worksheets() if w.id != tmp.id]:
            target.del_worksheet(ws)

        copied_worksheets = []
        for ws in source_sheets:
            copy_result = ws.copy_to(target_file_key)
            # gspread returns {"sheetId": <int>} for copy_to
            new_sheet_id = copy_result["sheetId"] if isinstance(copy_result, dict) else copy_result
            new_ws = target.get_worksheet_by_id(new_sheet_id)
            new_ws.update_title(ws.title)
            copied_worksheets.append(new_ws)

        # Reorder to match source order.
        target.reorder_worksheets(copied_worksheets + [tmp])

        # Remove temporary sheet.
        target.del_worksheet(tmp)

    def detect_changes(self, file_key: str, last_timestamp: datetime.datetime) -> bool:
        """
        Return True if the given spreadsheet was modified after last_timestamp.
        """
        modified_time = self.get_modified_time(file_key)
        modified_time = datetime.datetime.fromisoformat(modified_time)
        return modified_time > last_timestamp

    @rate_limited_with_retry()
    def weather_history_hours_lookup(self, latitude: float, longitude: float) -> dict:
        url = "https://weather.googleapis.com/v1/history/hours:lookup"
        params = {
            "location.latitude": latitude,
            "location.longitude": longitude,
            "hours": 24,
        }

        headers = {}
        if not self._weather_oauth_creds.valid:
            self._weather_oauth_creds.refresh(GoogleAuthRequest())
        headers["Authorization"] = f"Bearer {self._weather_oauth_creds.token}"

        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
