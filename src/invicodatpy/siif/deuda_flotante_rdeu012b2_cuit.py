#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rdeu012b2_cuit report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class DeudaFlotanteRdeu012b2Cuit(RPWUtils):
    """Read, process and write SIIF's rdeu012 report"""
    _REPORT_TITLE = 'DETALLE DE COMPROBANTES DE GASTOS ORDENADOS Y NO PAGADOS (DEUDA FLOTANTE)'
    _TABLE_NAME = 'deuda_flotante_rdeu012'
    _INDEX_COL = 'id'
    _FILTER_COL = 'mes_hasta'
    _SQL_MODEL = SIIFModel

    # --------------------------------------------------
    def from_external_report(self, pdf_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_pdf(pdf_path)
        self.df = df
        return df
        # read_title = df['2'].iloc[9]
        # if read_title == self._REPORT_TITLE:
        #     self.df = df
        #     self.transform_df()
        # else:
        #     # Future exception raise
        #     pass

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
            )

        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        ) 

        df = df >> \
            dplyr.mutate(
                fecha = dplyr.if_else(f.fecha_aprobado > f.fecha_hasta,
                                        f.fecha_hasta, f.fecha_aprobado)
            )

        # CYO aprobados en enero correspodientes al ejercicio anterior
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce') 
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        condition = ((df['mes_hasta'].str[0:2] == '01') & 
                    (df['nro_entrada'].astype(int) > 1500))
        df.loc[condition, 'fecha'] = (
            (pd.to_numeric(df['mes_hasta'].loc[condition].str[-4:]) - 1).astype(str) + "-12-31")

        df['fecha'] = pd.to_datetime(
            df['fecha'],  format='%Y-%m-%d', errors='coerce'
        ) 
        
        df['ejercicio'] = df['fecha'].dt.year.astype(str)
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')

        df = df >>\
            dplyr.mutate(
                nro_comprobante = (f['nro_entrada'].str.zfill(5) + 
                                '/' + f['mes'].str[-2:]),
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha, 
                f.mes_hasta, f.fuente,
                f.cta_cte, f.nro_comprobante,
                f.importe, f.saldo,
                f.cuit, f.beneficiario,
                f.glosa, f.nro_expte,
                f.nro_entrada, f.nro_origen,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rdeu012b2_cuit report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "pdf_file",
        default='202212-rdeu012b2_Cuit.pdf',
        type=str,
        help = "SIIF' rdeu012b2_cuit.pdf report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rdeu012 = DeudaFlotanteRdeu012b2Cuit()
    siif_rdeu012.from_external_report(dir_path + '/' + args.file)
    siif_rdeu012.test_sql(dir_path + '/test.sqlite')
    # siif_rdeu012.to_sql(dir_path + '/siif.sqlite')
    # siif_rdeu012.print_tidyverse()
    # siif_rdeu012.from_sql(dir_path + '/siif.sqlite')
    # siif_rdeu012.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.deuda_flotante_rdeu012b2_cuit