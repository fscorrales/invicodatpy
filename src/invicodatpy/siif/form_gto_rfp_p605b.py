#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rfp_p605b report
"""

__all__ = ['FormGtoRfpP605b']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..models.siif_model import SIIFModel
from .connect_siif import ConnectSIIF, ReportCategory


@dataclass
class FormGtoRfpP605b(ConnectSIIF):
    """
    A class for handling SIIF's 'rfp_p605b' report.

    This class extends the functionality of 'ConnectSIIF' and is specifically designed
    for downloading, processing, and writing SIIF's 'rfp_p605b' report. It provides
    methods for downloading the report, reading it from an external file, transforming
    the data, and storing it in an SQLite database.

    Args:
        siif (ConnectSIIF, optional): An instance of 'ConnectSIIF' for connecting to
            the SIIF system. If not provided, the download functionality will be
            disabled.

    Attributes:
        _REPORT_TITLE (str): The title of the SIIF report.
        _TABLE_NAME (str): The name of the table used to store the data in the SQLite
            database.
        _INDEX_COL (str): The name of the column used as the index in the data table.
        _FILTER_COL (str): The name of the column used for filtering data.
        _SQL_MODEL (SIIFModel): An instance of 'SIIFModel' for handling SQL operations.

    Methods:
        download_report(dir_path: str, ejercicios: Union[str, List[str]] = str(dt.datetime.now().year)):
            Downloads the 'rfp_p605b' report from SIIF.

        from_external_report(xls_path: str) -> pd.DataFrame:
            Reads the 'rfp_p605b' report from an external Excel file.

        transform_df() -> pd.DataFrame:
            Transforms the read Excel data into a tidy format.

    Example:
        To download, process, and store the 'rfp_p605b' report:

        ```python
        siif_connection = ConnectSIIF(username='your_username', password='your_password')
        siif_rfp_p605b = FormGtoRfpP605b(siif=siif_connection)
        siif_rfp_p605b.download_report(dir_path='/path/to/save', ejercicios='2024')
        siif_connection.disconnect()
        ```
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='rfp_p605b'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='form_gto_rfp_p605b'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='ejercicio'
    )
    _SQL_MODEL:SIIFModel = field(
        init=False, repr=False, default=SIIFModel
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        """
        Download the 'rfp_p605b' report from SIIF.

        This method downloads the 'rfp_p605b' report from the SIIF system for the specified
        exercise(s) and saves it to the provided directory path. It requires an active
        connection to the SIIF system, provided via the 'siif' attribute when initializing
        the class.

        Args:
            dir_path (str): The directory path where the downloaded report will be saved.
            ejercicios (Union[str, List[str]], optional): The exercise(s) for which to
                download the report. It can be a single exercise as a string or a list
                of exercises. Default is the current year.

        Example:
            To download the 'rfp_p605b' report for the year 2024:

            ```python
            siif_connection = ConnectSIIF(username='your_username', password='your_password')
            siif_rfp_p605b = FormGtoRfpP605b(siif=siif_connection)
            siif_rfp_p605b.download_report(dir_path='/path/to/save', ejercicios='2024')
            siif_connection.disconnect()
            ```
        """
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Formulacion)
            self.select_specific_report_by_id('890')
            
            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
            )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnEjecutarReporte']"
            )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            btn_xls.click()

            # Form submit
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= (dt.datetime.now().year + 1):
                    # Ejercicio
                    input_ejercicio.clear()
                    input_ejercicio.send_keys(ejercicio)
                    btn_get_reporte.click()
                    self.rename_report(dir_path, 'rfp_p605b.xls', ejercicio + '-rfp_p605b.xls')
                    self.download_file_procedure()
            time.sleep(1)

            # Going back to reports list
            self.go_back_to_reports_list()

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.disconnect()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """
        Read data from an external 'rfp_p605b' report.

        This method reads data from an external Excel file containing the 'rfp_p605b'
        report. It performs the reading operation and checks if the report's title matches
        the expected title. If the titles match, the data is stored in the class attribute
        'df'.

        Args:
            xls_path (str): The path to the external Excel file containing the report.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the 'rfp_p605b' report data.

        Example:
            To read data from an external 'rfp_p605b' report:

            ```python
            siif_rfp_p605b = FormGtoRfpP605b()
            siif_rfp_p605b.from_external_report('/path/to/report.xls')
            ```
        """
        df = self.read_xls(xls_path)
        read_title = df['37'].iloc[8]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass
        return self.df

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """
        Transform the read 'rfp_p605b' report data.

        This method transforms the data read from the 'rfp_p605b' report into a tidy format.
        It performs various data cleaning and restructuring operations and stores the
        transformed data in the 'df' attribute.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the transformed 'rfp_p605b' report
            data.

        Example:
            To transform the read report data:

            ```python
            siif_rfp_p605b = FormGtoRfpP605b()
            siif_rfp_p605b.from_external_report('/path/to/report.xls')
            siif_rfp_p605b.transform_df()
            ```
        """
        df = self.df
        df['ejercicio'] = df.iloc[13,1][-4:]
        df = df.drop(range(22))

        df['programa'] = np.where(
            df['3'].str[0:8] == 'Programa', 
            df['3'].str[22:], None
        )
        df['programa'] = df['programa'].fillna(method='ffill')
        df['prog'] = df['programa'].str[:2]
        df['prog'] = df['prog'].str.strip()
        df['desc_prog'] = df['programa'].str[3:]
        df['desc_prog'] = df['desc_prog'].str.strip()
        df['subprograma'] = np.where(
            df['3'].str[0:11] == 'SubPrograma', 
            df['3'].str[19:], None
        )
        df['proyecto'] = np.where(
            df['3'].str[0:8] == 'Proyecto', 
            df['3'].str[24:], None
        )
        df['actividad'] = np.where(
            df['3'].str[0:9] == 'Actividad', 
            df['3'].str[20:], None
        )
        df['grupo'] = np.where(
            df['10'] != '', 
            df['10'].str[0:3], None
        )
        df['grupo'] = df['grupo'].fillna(method='ffill')
        df['partida'] = np.where(
            df['9'] != '', 
            df['9'], None
        )
        df['fuente_11'] = df['22']
        df['fuente_10'] = df['19']
        df = df.loc[:,[
            'ejercicio', 'prog', 'desc_prog', 'subprograma', 'proyecto',
            'actividad', 'grupo', 'partida', 'fuente_11', 'fuente_10',
        ]]
        df = df.dropna(subset=['subprograma','proyecto','actividad','partida'], how='all')
        df['subprograma'] = df['subprograma'].fillna(method='ffill')
        df = df.dropna(subset=['proyecto','actividad','partida'], how='all')
        df['proyecto'] = df['proyecto'].fillna(method='ffill')
        df = df.dropna(subset=['actividad','partida'], how='all')
        df['actividad'] = df['actividad'].fillna(method='ffill')
        df = df[df['partida'].str.len() == 3]
        df['sub'] = df['subprograma'].str[:2]
        df['sub'] = df['sub'].str.strip()
        df['desc_subprog'] = df['subprograma'].str[3:]
        df['desc_subprog'] = df['desc_subprog'].str.strip()
        df['proy'] = df['proyecto'].str[:2]
        df['proy'] = df['proy'].str.strip()
        df['desc_proy'] = df['proyecto'].str[3:]
        df['desc_proy'] = df['desc_proy'].str.strip()
        df['act'] = df['actividad'].str[:2]
        df['act'] = df['act'].str.strip()
        df['desc_act'] = df['actividad'].str[3:]
        df['desc_act'] = df['desc_act'].str.strip()
        df['fuente_10'] = df['fuente_10'].astype(float)
        df['fuente_11'] = df['fuente_11'].astype(float)
        df['fuente'] = np.select(
            [
                df['fuente_10'].astype(int) > 0,
                df['fuente_11'].astype(int) > 0,
            ],
            ['10', '11']
        )
        df['formulado'] = df['fuente_10'] + df['fuente_11']
        df['prog'] = df['prog'].str.zfill(2)
        df['sub'] = df['sub'].str.zfill(2)
        df['proy'] = df['proy'].str.zfill(2)
        df['act'] = df['act'].str.zfill(2)
        df['estructura'] = (
            df['prog'] + '-' + df['sub'] + '-' + 
            df['proy'] + '-' + df['act'] + '-' +
            df['partida']
        )
        df = df.loc[:,[
            'ejercicio', 'estructura','fuente','prog', 'desc_prog', 
            'sub', 'desc_subprog', 'proy', 'desc_proy', 
            'act', 'desc_act', 'grupo', 'partida', 'formulado'
        ]]
        df = df.rename(columns={
            'prog':'programa', 
            'sub':'subprograma', 
            'proy':'proyecto',
            'act':'actividad',
        })   

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """
    Parse command line arguments.

    This function parses the command line arguments provided when running the script. It defines several
    command line options, including options for specifying the input file, whether to perform a download,
    and SIIF login credentials.

    Returns:
        argparse.Namespace: An object containing the parsed command line arguments.

    Example:
        To parse command line arguments:

        ```python
        args = get_args()
        print(args.username)  # Accessing the 'username' argument value
        ```

    """
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rfp_p605b report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rfp_p605b.xls report. Must be in the same folder")

    parser.add_argument('--download', action='store_true')
    parser.add_argument('--no-download', dest='download', action='store_false')
    parser.set_defaults(download=True)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'Username',
        default = '',
        type=str,
        help = "Username to log in SIIF")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'Password',
        default = '',
        type=str,
        help = "Password to log in SIIF")

    parser.add_argument(
        '-e', '--ejercicio', 
        metavar = 'Ejercicio',
        default = '2024',
        type=str,
        help = "Ejercicio to download from SIIF")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """
    Main script execution.

    This function serves as the entry point for the script. It performs the main functionality of the script,
    which includes downloading, processing, and writing SIIF's 'rfp_p605b' report. It uses the parsed command
    line arguments to determine the actions to be taken.

    Example:
        To run the main script:

        ```
        if __name__ == '__main__':
            main()
        ```

    """
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))

    if args.download:
        json_path = dir_path + '/siif_credentials.json'
        if args.username != '' and args.password != '':
            ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    ConnectSIIF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif = FormGtoRfpP605b()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif.disconnect()
    else:
        siif = FormGtoRfpP605b()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rfp_p605b.xls'

    siif.from_external_report(dir_path + '/' + filename)
    # siif.test_sql(dir_path + '/test.sqlite')
    siif.to_sql(dir_path + '/siif.sqlite')
    siif.print_tidyverse()
    siif.from_sql(dir_path + '/siif.sqlite')
    siif.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.form_gto_rfp_p605b --no-download
