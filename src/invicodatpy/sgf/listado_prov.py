#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SGF's 'Listado de Proveedores' report
"""

import argparse
import inspect
import os

import pandas as pd
# from datar import base, dplyr, f, tidyr

from ..models.sgf_model import SGFModel
from ..utils.rpw_utils import RPWUtils


class ListadoProv(RPWUtils):
    """Read, process and write SGF's 'Listado de Proveedores' report"""
    _REPORT_TITLE = 'Listado de Proveedores'
    _TABLE_NAME = 'listado_prov'
    _INDEX_COL = 'id'
    _FILTER_COL = ''
    _SQL_MODEL = SGFModel

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SGF's report"""
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
        df = self.df.iloc[:,9:16]
        df.columns = [
            "codigo", "desc_prov", "domicilio", "localidad",
            "telefono", "cuit", "condicion_iva"
        ]
        df['cuit']= df['cuit'].replace(to_replace='', value=None)

        df = df >> \
            tidyr.drop_na(f.cuit) >> \
            dplyr.mutate(
                cuit = base.gsub('-', '', f.cuit)
            ) >> \
            dplyr.select(
                f.codigo, f.cuit,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SGF's 'Listado de Proveedores' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='Listado de Proveedores.csv',
        type=str,
        help = "SGF' Listado de Proveedores.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_listado_prov = ListadoProv()
    sgf_listado_prov.from_external_report(dir_path + '/' + args.file)
    # sgf_listado_prov.test_sql(dir_path + '/test.sqlite')
    sgf_listado_prov.to_sql(dir_path + '/sgf.sqlite', True)
    sgf_listado_prov.print_tidyverse()
    sgf_listado_prov.from_sql(dir_path + '/sgf.sqlite')
    sgf_listado_prov.print_tidyverse()
    # print(sgf_listado_prov.df.head(10))
    # dir = '/home/kanou/IT/R Apps/R Gestion INVICO/invicoDB/Base de Datos/Sistema Gestion Financiera/Otros Reportes/Listado de Proveedores.csv'
    # sgf_listado_prov.update_sql_db(dir, dir_path + '/sgf.sqlite', True)

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.listado_prov
