#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SSCC's 'Listado de Imputaciones' report
"""

__all__ = ['ListadoImputaciones']

import argparse
import inspect
import os

import pandas as pd

from ..models.sscc_model import SSCCModel
from ..utils.rpw_utils import RPWUtils


class ListadoImputaciones(RPWUtils):
    """Read, process and write SGF's 'Listado de Imputaciones' report"""
    _REPORT_TITLE = 'Listado de Imputaciones'
    _TABLE_NAME = 'listado_imputaciones'
    _INDEX_COL = 'id'
    _FILTER_COL = ''
    _SQL_MODEL = SSCCModel

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SSCC's report"""
        df = self.read_csv(csv_path)
        read_title = df['1'].iloc[0]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read csv file"""
        df = self.df.iloc[:,6:10]
        df.columns = [
            "cod_imputacion", "imputacion", "tipo", "imputacion_fonavi",
        ]

        # add lead zeros to cod_imputacion
        df['cod_imputacion'] = df['cod_imputacion'].astype(str).str.zfill(3)

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SSCC's 'Listado de Imputaciones' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='Bancos - Listado de Imputaciones.csv',
        type=str,
        help = "SSCC' Listado de Imputaciones.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_listado_imputaciones = ListadoImputaciones()
    sgf_listado_imputaciones.from_external_report(dir_path + '/' + args.file)
    # sgf_listado_imputaciones.test_sql(dir_path + '/test.sqlite')
    sgf_listado_imputaciones.to_sql(dir_path + '/sscc.sqlite', True)
    sgf_listado_imputaciones.print_tidyverse()
    sgf_listado_imputaciones.from_sql(dir_path + '/sscc.sqlite')
    sgf_listado_imputaciones.print_tidyverse()
    # print(sgf_listado_imputaciones.df.head(10))
    # dir = '/home/kanou/IT/R Apps/R Gestion INVICO/invicoDB/Base de Datos/Sistema Gestion Financiera/Otros Reportes/Listado de Proveedores.csv'
    # sgf_listado_imputaciones.update_sql_db(dir, dir_path + '/sgf.sqlite', True)

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sscc.listado_imputaciones
