from dotenv import load_dotenv
import argparse
import datetime
from zoneinfo import ZoneInfo

from APIs.google_spreadsheets import GoogleAPI
from APIs.owncloud import OwnCloudAPI
from processing.utils import get_last_data_timestamp, get_last_config_timestamp, load_config_versions
from processing.xml import FormXMLParser
from processing.process import process_site


def main():
    load_dotenv('CONFIG.env')

    google_api = GoogleAPI()
    owncloud_api = OwnCloudAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    last_run_timestamp = get_last_data_timestamp()
    last_config_timestamp = get_last_config_timestamp()

    # download new submitted sites
    subfolders = owncloud_api.get_new_folders(last_run_timestamp)

    # download configs
    owncloud_api.get_new_config_files("logsheets", "downloaded_configs", last_config_timestamp)

    # load configs
    configs = load_config_versions("downloaded_configs")

    data = dict()
    
    # process the new sites
    for subfolder in subfolders:
        files = owncloud_api.get_remote_files(subfolder)
        for filename, content in files:
            if filename != "site_metadata.xml":
                xml = FormXMLParser()
                xml.parse_string(content)
                config = configs[xml.form_id][xml.logsheet_version]

                output = process_site(xml, config)
                output["Site ID"] = xml.site_id
                output["Submission date"] = xml.submitted_at
                
                data[xml.form_id] = data.get(xml.form_id, []) + [output]

        # consider special cases, such as merging LSI 14 scoring sheets


    # processed_submissions = process_submissions(submissions, config.get('postprocessing', dict()))

    # # store war submissions to OwnCloud for backup
    # print('\tBacking up submissions to OwnCloud...')
    # store_submissions_to_oc(submissions)
    # processed_df = pd.DataFrame(processed_submissions)

    # # store to Google sheet
    # print('\tStoring submissions in Google sheets...')
    # row_dicts = processed_df.to_dict(orient="records")

    # for row in row_dicts:
    #     run_actions(row, config.get('actions', dict()), jotform_api)

    # google_api.add_rows(config['target_sheet'], config['worksheet'], row_dicts)

    # if 'backup_sheet' in config:
    #     google_api.add_rows(config['backup_sheet'], config['worksheet'], row_dicts)

    # and update the last run timestamp


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Process new logsheet submissions')

    args_parser._action_groups.pop()
    optional = args_parser.add_argument_group('optional arguments')
    
    args = args_parser.parse_args()
    main()
