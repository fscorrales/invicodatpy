#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rf602 report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .siif import SIIF

class PptoGtosFteRf602(SIIF):
    """Read, process and write SIIF's rf602 report"""
    _REPORT_TITLE = 'DETALLE DE LA EJECUCION PRESUESTARIA'
    _TABLE_NAME = 'ppto_gtos_fte_rf602'
    _INDEX_COL = 'id'
    _FILTER_COL = 'ejercicio'

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['2'].iloc[5][:-5] 
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df >> \
            dplyr.transmute(
                ejercicio = f['2'].iloc[5][-4:],
                programa = f['2'].str.zfill(2),
                subprograma=  f['3'].str.zfill(2),
                proyecto =  f['6'].str.zfill(2),
                actividad =  f['7'].str.zfill(2),
                partida = f['8'],
                grupo = f['8'].str[1] + '00',
                fuente = f['9'],
                org = f['10'],
                credito_original = f['13'],
                credito_vigente =  f['14'],
                comprometido = f['15'],
                ordenado = f['16'],
                saldo = f['18'],
                pendiente = f['20']
            ) >> \
            base.tail(-16) >> \
            dplyr.filter_(f.programa != '00') >> \
            tidyr.unite(
                'estructura',
                [f.programa, f.subprograma, f.proyecto,
                f.actividad, f.partida],
                sep='-', remove=False
            ) >> \
            dplyr.select(
                f.ejercicio, f.estructura, f.fuente,
                f.programa, f.subprograma, f.proyecto, 
                f.actividad, f.grupo, f.partida,
                dplyr.everything()
            )

        campos_modificar = ['credito_original', 'credito_vigente', 
        'comprometido', 'ordenado', 'saldo', 'pendiente']
        df[campos_modificar] = df[campos_modificar].astype(float)
        df.reset_index(inplace=True, drop=True)
        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rf602 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rf602.xls',
        type=str,
        help = "SIIF' rf602.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rf602 = PptoGtosFteRf602()
    siif_rf602.from_external_report(dir_path + '/' + args.file)
    siif_rf602.to_sql(dir_path + '/siif.sqlite')
    siif_rf602.print_tidyverse()
    siif_rf602.from_sql(dir_path + '/siif.sqlite')
    siif_rf602.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.ppto_gtos_fte_rf602 -f 2022-rf602.xls
