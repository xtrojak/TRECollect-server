import os
import sys
from dotenv import load_dotenv

from APIs.google_spreadsheets import GoogleAPI


def cleanup_spreadsheet(env_key: str) -> None:
    """
    Delete all data rows (but keep header row and its formatting)
    from every worksheet in the spreadsheet referenced by ENV[env_key].
    """
    load_dotenv("CONFIG.env")
    file_key = os.environ.get(env_key)
    if not file_key:
        raise SystemExit(f"Environment variable {env_key!r} is not set.")

    api = GoogleAPI()
    spreadsheet = api.client.open_by_key(file_key)

    for ws in spreadsheet.worksheets():
        # Clear everything below the first row, leaving headers (and their formatting) intact.
        # This uses A1 notation "2:<last_row>" to wipe rows 2..N.
        last_row = ws.row_count
        if last_row <= 1:
            continue
        ws.batch_clear([f"2:{last_row}"])
        print(f"Cleared data rows in sheet '{ws.title}' (kept header).")


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        prog = os.path.basename(argv[0] or "cleanup_sheets.py")
        print(f"Usage: python {prog} <ENV_VAR_WITH_SHEET_ID>")
        print("Example: python cleanup_sheets.py LSI_SHEET_LATEST_SUBMISSIONS_ID")
        raise SystemExit(1)

    env_key = argv[1]
    cleanup_spreadsheet(env_key)


if __name__ == "__main__":
    main(sys.argv)
