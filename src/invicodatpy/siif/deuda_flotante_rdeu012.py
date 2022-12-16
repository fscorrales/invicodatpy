#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rdeu012 report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .siif import SIIF

class DeudaFlotanteRdeu012(SIIF):
    """Read, process and write SIIF's rdeu012 report"""
    _REPORT_TITLE = 'DETALLE DE COMPROBANTES DE GASTOS ORDENADOS Y NO PAGADOS (DEUDA FLOTANTE)'
    _TABLE_NAME = 'deuda_flotante_rdeu012'
    _INDEX_COL = 'id'
    _FILTER_COL = 'mes_hasta'

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['2'].iloc[9]
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
        df['6'] = df['6'].replace(to_replace='TODOS', value='') 
        df.loc[df['6'] != '27', 'fuente'] = df['6']
        df['fecha_desde'] = df.iloc[15,2].split(' ')[2]
        df['fecha_hasta'] = df.iloc[15,2].split(' ')[6]
        df['fecha_desde'] = pd.to_datetime(
            df['fecha_desde'], format='%d/%m/%Y'
        )
        df['fecha_hasta'] = pd.to_datetime(
            df['fecha_hasta'], format='%d/%m/%Y'
        )
        df['mes_hasta'] = (df['fecha_hasta'].dt.month.astype(str).str.zfill(2) +
                            '/'+ df['fecha_hasta'].dt.year.astype(str))
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-13) >> \
            tidyr.fill(f.fuente) >> \
            tidyr.drop_na(f['2']) >> \
            tidyr.drop_na(f['18']) >> \
            dplyr.transmute(
                fuente = f.fuente,
                fecha_desde = f.fecha_desde,
                fecha_hasta = f.fecha_hasta,
                mes_hasta = f.mes_hasta,
                nro_entrada = f['2'],
                nro_comprobante = (f['nro_entrada'].str.zfill(5) + 
                                '/' + f['mes_hasta'].str[-2:]),
                nro_origen = f['4'],
                fecha_aprobado = f['7'],
                org_fin = f['9'],
                importe = base.as_double(f['10']),
                saldo = base.as_double(f['13']),
                nro_expte = f['14'],
                cta_cte = f['15'],
                glosa = f['17'],
                cuit = f['18'],
                beneficiario = f['19']
            ) >>\
            dplyr.select(
                f.mes_hasta, f.fecha_aprobado, f.fuente,
                f.cta_cte, f.nro_comprobante,
                f.importe, f.saldo,
                f.cuit, f.beneficiario,
                f.glosa, f.nro_expte,
                f.nro_entrada, f.nro_origen,
                dplyr.everything()
            )

        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        ) 

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rdeu012 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='202210-rdeu012.xls',
        type=str,
        help = "SIIF' rdeu012.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rdeu012 = DeudaFlotanteRdeu012()
    siif_rdeu012.from_external_report(dir_path + '/' + args.file)
    # siif_rdeu012.test_sql(dir_path + '/test.sqlite')
    siif_rdeu012.to_sql(dir_path + '/siif.sqlite')
    siif_rdeu012.print_tidyverse()
    siif_rdeu012.from_sql(dir_path + '/siif.sqlite')
    siif_rdeu012.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.deuda_flotante_rdeu012