#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Join rf602 (ppto_gtos_fte) with rf610 (ppto_gtos_desc)
"""

import argparse
import inspect
import os

import pandas as pd

from ..utils.print_tidyverse import PrintTidyverse
from .ppto_gtos_desc_rf610 import PptoGtosDescRf610
from .ppto_gtos_fte_rf602 import PptoGtosFteRf602


class JoinPptoGtosFteDesc():
    """Join rf602 (ppto_gtos_fte) with rf610 (ppto_gtos_desc)"""
    df:pd.DataFrame = None
    
    # --------------------------------------------------
    def from_external_report(
        self, ppto_fte_xls_path:str, ppto_desc_xls_path:str
    ) -> pd.DataFrame:
        self.df_ppto_fte = PptoGtosFteRf602().from_external_report(ppto_fte_xls_path)
        self.df_ppto_desc = PptoGtosDescRf610().from_external_report(ppto_desc_xls_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def from_sql(self, sql_path:str) -> pd.DataFrame:
        self.df_ppto_fte = PptoGtosFteRf602().from_sql(sql_path)
        self.df_ppto_desc = PptoGtosDescRf610().from_sql(sql_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def join_df(self) -> pd.DataFrame:
        df_ppto_desc_filtered = self.df_ppto_desc[[
            'ejercicio', 'estructura', 
            'desc_prog', 'desc_subprog', 'desc_proy', 
            'desc_act', "desc_gpo", "desc_part"
        ]]
        self.df = pd.merge(
            left=self.df_ppto_fte,
            right=df_ppto_desc_filtered,
            on=['ejercicio', 'estructura'],
            how='left'
        )
        return self.df

    # --------------------------------------------------
    def print_tidyverse(self):
        print(PrintTidyverse(self.df))

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Join rf602 (ppto_gtos_fte) with rf610 (ppto_gtos_desc)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--sql_file', 
        metavar = "sql_file",
        default='siif.sqlite',
        type=str,
        help = "SIIF' sqlite DataBase file name. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_join_ppto_gtos = JoinPptoGtosFteDesc()
    siif_join_ppto_gtos.from_sql(dir_path + '/' + args.sql_file)
    siif_join_ppto_gtos.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.join_ppto_gtos_fte_desc -f siif.sqlite