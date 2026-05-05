from dotenv import load_dotenv
import argparse
import datetime
import pandas as pd
from zoneinfo import ZoneInfo
import os

from APIs.google_spreadsheets import GoogleAPI
from APIs.owncloud import OwnCloudAPI
from processing.utils import get_last_data_timestamp, get_last_config_timestamp, \
                            load_config_versions, save_last_config_timestamp, \
                            save_last_data_timestamp, is_debug_submission
from processing.xml import FormXMLParser
from processing.process import process_site
from curation.curate_submissions import run_curation, curate_rows_per_sheet
from curation.output_rules import apply_output_rules, get_output_rules


def _curated_output_filename(local_filename: str, sheet_prefix: str) -> str:
    base, ext = os.path.splitext(local_filename)
    if not ext:
        ext = ".xlsx"
    return f"{base}_{sheet_prefix}_curated{ext}"


def main(args):
    load_dotenv('CONFIG.env')

    owncloud_api = OwnCloudAPI()

    google_api = None
    if not args.local:
        google_api = GoogleAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    last_run_timestamp = get_last_data_timestamp()
    last_config_timestamp = get_last_config_timestamp()

    raw_sheet_id = os.environ.get('RAW_SHEET_ID')
    raw_sheet_backup_id = os.environ.get('RAW_SHEET_BACKUP_ID')
    raw_sheet_debug_id = os.environ.get('RAW_SHEET_ID_DEV')

    print(f'>>> {now}')

    # download new submitted sites
    subfolders = owncloud_api.get_new_folders(last_run_timestamp)

    if len(subfolders) > 0:
        # download configs
        downloaded = owncloud_api.get_new_config_files("logsheets", "downloaded_configs", last_config_timestamp)
        if downloaded:
            save_last_config_timestamp(now)
        
        # load configs
        configs = load_config_versions("downloaded_configs")

        data = dict()
        debug_data = dict()

        logsheet_names = dict()
        
        # process the new sites
        for subfolder in subfolders:
            print(f'>>> Downloading site {subfolder}...')
            files = owncloud_api.get_remote_files(subfolder)
            for filename, content in files:
                if filename != "site_metadata.xml":
                    xml = FormXMLParser()
                    xml.parse_string(content)
                    config = configs[xml.form_id][xml.logsheet_version]

                    # store the logsheet name for Google spreadsheet names
                    logsheet_names[xml.form_id] = config['name']

                    output = process_site(xml, config)
                    output["Site ID"] = xml.site_id
                    output["Submission date"] = xml.submitted_at

                    if is_debug_submission(subfolder):
                        debug_data[xml.form_id] = debug_data.get(xml.form_id, []) + [output]
                    else:
                        data[xml.form_id] = data.get(xml.form_id, []) + [output]

        for dataset, sheet_id, backup_id in [
            (data, raw_sheet_id, raw_sheet_backup_id),
            (debug_data, raw_sheet_debug_id, None),
        ]:
            print(f'>>> Processing {"production" if backup_id else "debug"} data...')
            for form_id, submissions in dataset.items():
                print(f'>>> Processing form {form_id} with {len(submissions)} submissions...')

                processed_df = pd.DataFrame(submissions)

                if args.local:
                    # Store to a local Excel file (one sheet per logsheet name)
                    print(f'\tStoring submissions locally in {args.local}...')
                    sheet_name = logsheet_names[form_id]
                    # Append/replace sheet if file exists, otherwise create new file
                    if os.path.exists(args.local):
                        mode = "a"
                        if_sheet_exists = "replace"
                    else:
                        mode = "w"
                        if_sheet_exists = None
                    
                    with pd.ExcelWriter(args.local, engine="openpyxl", mode=mode, if_sheet_exists=if_sheet_exists) as writer:
                        processed_df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    # store to Google sheet
                    print('\tStoring submissions in Google sheets...')
                    row_dicts = processed_df.to_dict(orient="records")

                    google_api.add_rows(sheet_id, logsheet_names[form_id], row_dicts)
                    if backup_id:
                        google_api.add_rows(backup_id, logsheet_names[form_id], row_dicts)

        # Curate production data:
        # - normal mode: write to Google target sheets
        # - local mode: write to local Excel file with "_curated" suffix
        if data:
            for sheet_prefix, target_sheet_id in [
                ("LSI", "LSI_SHEET_LATEST_SUBMISSIONS_ID"),
                ("Comadre", "COMADRE_SHEET_LATEST_SUBMISSIONS_ID"),
            ]:
                owncloud_images_token = os.environ.get('OWNCLOUD_IMAGES_TOKEN')
                if args.local:
                    if not owncloud_images_token:
                        print('>>> Missing OWNCLOUD_IMAGES_TOKEN, skipping local curation output.')
                    else:
                        print(f'>>> Running curation on {sheet_prefix} data (local mode)...')
                        raw_rows = {}
                        for form_id, rows in data.items():
                            sheet_name = logsheet_names.get(form_id)
                            if not sheet_name or not rows:
                                continue
                            raw_rows[sheet_name] = pd.DataFrame(rows)

                        curated = curate_rows_per_sheet(raw_rows, owncloud_images_token, sheet_prefix)
                        rows_to_write, _ = apply_output_rules(curated, {}, get_output_rules())

                        curated_file = _curated_output_filename(args.local, sheet_prefix)
                        print(f'>>> Writing curated output locally to {curated_file}...')
                        for sheet_name, df in rows_to_write.items():
                            if df.empty:
                                continue
                            if os.path.exists(curated_file):
                                mode = "a"
                                if_sheet_exists = "replace"
                            else:
                                mode = "w"
                                if_sheet_exists = None
                            with pd.ExcelWriter(curated_file, engine="openpyxl", mode=mode, if_sheet_exists=if_sheet_exists) as writer:
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    lsi_target_sheet_id = os.environ.get(target_sheet_id)
                    if lsi_target_sheet_id and owncloud_images_token:
                        print(f'>>> Running curation on {sheet_prefix} data...')
                        run_curation(data, logsheet_names, google_api, lsi_target_sheet_id, owncloud_images_token, sheet_prefix)
        else:
            print('>>> No production data to curate.')

        # and update the last run timestamp
        print(f'>>> Updating last run timestamp...')
        save_last_data_timestamp(now)
    else:
        print(f'>>> No new submissions found.')

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Process new logsheet submissions')

    args_parser._action_groups.pop()
    optional = args_parser.add_argument_group('optional arguments')
    optional.add_argument(
        '--local',
        metavar='FILENAME',
        help='Store output into a local Excel file instead of Google Sheets',
    )

    args = args_parser.parse_args()
    main(args)
