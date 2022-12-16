#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rfondo07tp report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class ResumenFdosRfondo07tp(RPWUtils):
    """Read, process and write SIIF's rfondo07tp report"""
    _REPORT_TITLE = 'RESUMEN DE FONDOS DEL EJERCICIO'
    _TABLE_NAME = 'resumen_fdos_rfondo07tp'
    _INDEX_COL = 'id'
    _FILTER_COL = ['mes', 'tipo_comprobante']
    _SQL_MODEL = SIIFModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['1'].iloc[4][:-5]
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
        df['ejercicio'] = df.iloc[4,1][-4:]
        df['tipo_comprobante'] = df.iloc[11,2].split(':')[2]
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-19) >> \
            tidyr.drop_na(f['10']) >> \
            dplyr.transmute(
                ejercicio = f.ejercicio,
                tipo_comprobante = base.trimws(f.tipo_comprobante),
                fecha = f['10'],
                mes =  f.fecha.str[5:7] + '/' + f.ejercicio,
                nro_fondo = f['3'],
                glosa = base.trimws(f['6']),
                ingresos = base.as_double(f['12']),
                egresos = base.as_double(f['15']),
                saldo = base.as_double(f['18']),
                nro_comprobante = f['nro_fondo'].str.zfill(5) + '/' + f['mes'].str[-2:]
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        df = df >>\
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.tipo_comprobante, f.nro_comprobante,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rfondo07tp report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rfondo07tp (PA6).xls',
        type=str,
        help = "SIIF' rfondo07tp.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rci02 = ResumenFdosRfondo07tp()
    siif_rci02.from_external_report(dir_path + '/' + args.file)
    # siif_rci02.test_sql(dir_path + '/test.sqlite')
    siif_rci02.to_sql(dir_path + '/siif.sqlite')
    siif_rci02.print_tidyverse()
    siif_rci02.from_sql(dir_path + '/siif.sqlite')
    siif_rci02.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.resumen_fdos_rfondo07tp