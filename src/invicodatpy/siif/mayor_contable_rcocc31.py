#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rcocc31 report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class MayorContableRcocc31(RPWUtils):
    """Read, process and write SIIF's rcocc31 report"""
    _REPORT_TITLE = 'DETALLES DE MOVIMIENTOS CONTABLES'
    _TABLE_NAME = 'mayor_contable_rcocc31'
    _INDEX_COL = 'id'
    _FILTER_COL = ['mes', 'cta_contable']
    _SQL_MODEL = SIIFModel


    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['2'].iloc[9][:33]
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
        df['ejercicio'] = df.iloc[3,1][-4:]
        df['cta_contable'] = (df.iloc[10,6] + '-' + 
        df.iloc[10,11] + '-' + df.iloc[10,12])
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-20) >> \
            tidyr.drop_na(f['3']) >> \
            dplyr.transmute(
                ejercicio = f.ejercicio,
                cta_contable = f.cta_contable, 
                nro_entrada = f['3'], 
                fecha_aprobado = f['9'],
                auxiliar_1 = f['14'],
                auxiliar_2 = f['19'],
                tipo_comprobante = f['21'],
                debitos = base.as_double(f['24']), 
                creditos = base.as_double(f['25']),
                saldo = base.as_double(f['27'])
            )

        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        )
        # df['fecha'] = df['fecha_aprobado'].dt.year.astype(str).apply(
        #     lambda x: df['fecha_aprobado'] 
        #     if x == df['ejercicio'] 
        #     else pd.to_datetime(df['ejercicio'] + '-12-31', format='%Y-%m-%d')
        #     )
        df.loc[df['fecha_aprobado'].dt.year.astype(str) == df['ejercicio'], 'fecha'] = df['fecha_aprobado']
        df.loc[df['fecha_aprobado'].dt.year.astype(str) != df['ejercicio'], 'fecha'] = pd.to_datetime(
            df['ejercicio'] + '-12-31', format='%Y-%m-%d'
        )

        df = df >>\
            dplyr.mutate(
                mes = df['fecha'].dt.month.astype(str).str.zfill(2) + '/' + df['ejercicio']
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha, f.fecha_aprobado,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rcocc31 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rcocc31 (1112-2-6 Banco).xls',
        type=str,
        help = "SIIF' rcocc31.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rcocc31 = MayorContableRcocc31()
    siif_rcocc31.from_external_report(dir_path + '/' + args.file)
    # siif_rcocc31.test_sql(dir_path + '/test.sqlite')
    siif_rcocc31.to_sql(dir_path + '/siif.sqlite')
    siif_rcocc31.print_tidyverse()
    siif_rcocc31.from_sql(dir_path + '/siif.sqlite')
    siif_rcocc31.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.mayor_contable_rcocc31