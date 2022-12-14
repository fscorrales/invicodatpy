#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rci02 report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .siif import SIIF

class ComprobantesRecRci02(SIIF):
    """Read, process and write SIIF's rci02 report"""
    _REPORT_TITLE = 'RESUMEN DIARIO DE COMPROBANTES DE RECURSOS'
    _TABLE_NAME = 'comprobantes_rec_rci02'
    _INDEX_COL = 'id'
    _FILTER_COL = ['mes']

    # --------------------------------------------------
    def from_siif_xls_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['25'].iloc[6]
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
        df['ejercicio'] = df.iloc[3,34]
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-22) >> \
            tidyr.drop_na(f['2']) >> \
            dplyr.transmute(
                ejercicio = f.ejercicio,
                fecha = f['17'],
                mes = f.fecha.str[5:7] + '/' + f.ejercicio,
                fuente = f['6'],
                cta_cte = f['28'], 
                nro_entrada = f['2'],
                importe = base.as_double(f['23']),  
                glosa = f['32'], 
                es_remanente = base.grepl("REMANENTE", f.glosa),
                es_invico = base.grepl("%", f.glosa),
                es_verificado = dplyr.if_else(f['42'] == 'S', True, False),
                clase_reg = f['10'],
                clase_mod = f['13']
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        df = df >>\
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rci02 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rci02.xls',
        type=str,
        help = "SIIF' rci02.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rci02 = ComprobantesRecRci02()
    siif_rci02.from_siif_xls_report(dir_path + '/' + args.file)
    # siif_rci02.test_sql(dir_path + '/test.sqlite')
    siif_rci02.to_sql(dir_path + '/siif.sqlite')
    siif_rci02.print_tidyverse()
    siif_rci02.from_sql(dir_path + '/siif.sqlite')
    siif_rci02.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.comprobantes_rec_rci02