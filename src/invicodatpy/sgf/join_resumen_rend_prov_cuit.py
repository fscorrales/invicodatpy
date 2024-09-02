#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Join Resumen Rend Prov y Listado Proveedores SGF
"""

__all__ = ['JoinResumenRendProvCuit']

import argparse
import inspect
import os

import pandas as pd

from ..utils.print_tidyverse import PrintTidyverse
from .listado_prov import ListadoProv
from .resumen_rend_prov import ResumenRendProv


class JoinResumenRendProvCuit():
    """Join Resumen Rend Prov y Listado Proveedores SGF"""
    df:pd.DataFrame = None
    
    # --------------------------------------------------
    def from_external_report(
        self, resumend_rend_csv_path:str, listado_prov_csv_path:str
    ) -> pd.DataFrame:
        self.df_resumen_rend = ResumenRendProv().from_external_report(resumend_rend_csv_path)
        self.df_listado_prov = ListadoProv().from_external_report(listado_prov_csv_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def from_sql(self, sql_path:str) -> pd.DataFrame:
        self.df_resumen_rend = ResumenRendProv().from_sql(sql_path)
        self.df_listado_prov = ListadoProv().from_sql(sql_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def join_df(self) -> pd.DataFrame:
        df_listado_prov_filtered = self.df_listado_prov[[
            'cuit', 'desc_prov'
        ]]
        self.df = pd.merge(
            left=self.df_resumen_rend,
            right=df_listado_prov_filtered,
            left_on='beneficiario', right_on='desc_prov',
            how='left'
        )
        self.df.drop(['desc_prov'], axis='columns', inplace=True)
        return self.df

    # --------------------------------------------------
    def print_tidyverse(self):
        print(PrintTidyverse(self.df))

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Join Resumen Rend Prov y Listado Proveedores SGF",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--sql_file', 
        metavar = "sql_file",
        default='sgf.sqlite',
        type=str,
        help = "SGF' sqlite DataBase file name. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_join_resumen_rend_cuit = JoinResumenRendProvCuit()
    sgf_join_resumen_rend_cuit.from_sql(dir_path + '/' + args.sql_file)
    sgf_join_resumen_rend_cuit.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.join_resumen_rend_prov_cuit -f sgf.sqlite