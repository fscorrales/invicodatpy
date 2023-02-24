#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Upload DataFrame to Google Sheets
Source:
    - https://towardsdatascience.com/using-python-to-push-your-pandas-dataframe-to-google-sheets-de69422508f
    - https://medium.com/@jb.ranchana/write-and-append-dataframes-to-google-sheets-in-python-f62479460cf0
    - https://medium.com/@vince.shields913/reading-google-sheets-into-a-pandas-dataframe-with-gspread-and-oauth2-375b932be7bf
"""
import argparse
from dataclasses import dataclass, field

import gspread #https://docs.gspread.org/en/latest/index.html#
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# --------------------------------------------------
@dataclass
class GoogleSheets():
    """Upload DataFrame to Google Sheets
    :param path_credentials_file: json file download from Google
    """
    path_credentials_file:str
    credentials: ServiceAccountCredentials = field(
        default=None, init=False, repr=False
    )
    # --------------------------------------------------
    def __post_init__(self):
        self.authorize_access()

    # --------------------------------------------------
    def authorize_access(self):
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.path_credentials_file, scope)
        self.gc = gspread.authorize(self.credentials)

    # --------------------------------------------------
    """Method to upload DataFrame to Google Sheets
    :param df: pandas DataFrame to upload
    :param spreadsheet_key: can be found in the URL of a 
    previously created sheet
    :param wks_name: Worksheet name
    """
    def to_google_sheets(
        self, df:pd.DataFrame, 
        spreadsheet_key:str, wks_name:str = 'Hoja 1',
        col_names:bool = True, row_names:bool = False
    ):
        sheet = self.gc.open_by_key(spreadsheet_key)
        worksheet = sheet.worksheet(wks_name)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    #https://github.com/maybelinot/df2gspread/issues/41#issuecomment-1154527949

    # def upload_pandas_df(self, df:pd.DataFrame):
    #   values = [df.columns.values.tolist()]
    #   values.extend(df.values.tolist())
    #   sheet.values_update(
    #     self.title,
    #     params = { 'valueInputOption': 'USER_ENTERED' },
    #     body = { 'values': values }
    #   )

    # gspread.Worksheet.upload_pandas_df = upload_pandas_df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Upload DataFrame to Google Sheets",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        'spreadsheet_key', 
        metavar = "spreadsheet_key",
        type=str,
        help = "can be found in the URL of a previously created sheet")

    parser.add_argument(
        '-c', '--credentials', 
        metavar = "json_credentials",
        default='google_credentials.json',
        type=str,
        help = "Google's json file credentials name. Must be in the same folder")

    parser.add_argument(
        '-w', '--wks_name', 
        metavar = "worksheet_name",
        default='Hoja 1',
        type=str,
        help = "worksheet_name")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)

    args = get_args()

    gs = GoogleSheets(args.credentials)
    gs.to_google_sheets(
        df,  
        spreadsheet_key = args.spreadsheet_key,
        wks_name = args.wks_name
    )
    print('Upload complete')

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src/invicodatpy/utils
    # python google_sheets.py 119DPlbkDm-MQ3-R4K8VGR_BNkHpDM1sgTn5CaiRc418