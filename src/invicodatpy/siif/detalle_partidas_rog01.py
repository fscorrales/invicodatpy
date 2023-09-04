#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rog01 report
"""

import argparse
import inspect
import os

import pandas as pd
import numpy as np
from datar import base, dplyr, f, tidyr

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class DetallePartidasRog01(RPWUtils):
    """Read, process and write SIIF's rog01 report"""
    _REPORT_TITLE = 'rog01'
    _TABLE_NAME = 'detalle_partidas'
    _INDEX_COL = 'partida'
    _FILTER_COL = ''
    _SQL_MODEL = SIIFModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[7, 16]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df
        df = df.iloc[15:,[2, 4, 5, 6, 7, 11]]
        df.columns = [
            'grupo', 'part_parcial', 'desc_grupo', 
            'partida', 'desc_part_parcial', 'desc_partida'
        ]
        df.replace('', np.nan, inplace=True)
        df.grupo.fillna(value=None, method="ffill", inplace=True)
        df.part_parcial.fillna(value=None, method="ffill", inplace=True)
        df.desc_grupo.fillna(value=None, method="ffill", inplace=True)
        df.desc_part_parcial.fillna(value=None, method="ffill", inplace=True)
        df.dropna(axis=0, how='any', inplace=True)
        # df = df >>\
        #     dplyr.select(
        #         f.ejercicio, f.mes, f.fecha,
        #         dplyr.everything()
        #     )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rog01 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='detalle_partidas.xls',
        type=str,
        help = "SIIF' rog01.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rog01 = DetallePartidasRog01()
    siif_rog01.from_external_report(dir_path + '/' + args.file)
    # siif_rog01.test_sql(dir_path + '/test.sqlite')
    siif_rog01.to_sql(dir_path + '/siif.sqlite', True)
    siif_rog01.print_tidyverse()
    siif_rog01.from_sql(dir_path + '/siif.sqlite')
    siif_rog01.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.detalle_partidas_rog01