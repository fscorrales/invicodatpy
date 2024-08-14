#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rdeu012b2_cuit report
"""

import argparse
import inspect
import os

import pandas as pd
# from datar import dplyr, f

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class DeudaFlotanteRdeu012b2C(RPWUtils):
    """Read, process and write SIIF's rdeu012 report"""
    _REPORT_TITLE = 'DETALLE DE COMPROBANTES DE GASTOS ORDENADOS Y NO PAGADOS (DEUDA FLOTANTE)'
    _TABLE_NAME = 'deuda_flotante_rdeu012b2_c'
    _INDEX_COL = 'id'
    _FILTER_COL = 'mes_hasta'
    _SQL_MODEL = SIIFModel

    # --------------------------------------------------
    def from_external_report(self, path:str) -> pd.DataFrame:
        """"Read from pdf or csv SIIF's report"""
        ext = os.path.splitext(path)[1]
        if ext == '.pdf':
            pass
            # df = self.read_pdf(path)
            # df = df.iloc[2:]
            # self.df = df
            # self.transform_df()
        elif ext == '.csv':
            df = self.read_csv(path)    
            read_title = df['4'].iloc[3]
            if read_title == self._REPORT_TITLE:
                df = df.iloc[:,0:9]
                self.df = df
                self.transform_df()                
        else:
            return None
        # read_title = df['2'].iloc[9]
        # if read_title == self._REPORT_TITLE:
        #     self.df = df
        #     self.transform_df()
        # else:
        #     # Future exception raise
        #     pass
        return self.df

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df
        df['fecha_desde'] = df['0'].iloc[6][6:16]
        df['fecha_desde'] = pd.to_datetime(
            df['fecha_desde'], format='%d/%m/%Y'
        ) 
        df['fecha_hasta'] = df['0'].iloc[6][-10:]
        df['fecha_hasta'] = pd.to_datetime(
            df['fecha_hasta'], format='%d/%m/%Y'
        )
        df['ejercicio'] = df['fecha_hasta'].dt.year.astype(str)
        df['mes_hasta'] = df['fecha_hasta'].dt.strftime('%m/%Y')
        df['entidad'] = df.loc[df['0'] == 'Entidad']['3']
        df["entidad"].fillna(method='ffill', inplace = True)
        df = df.iloc[9:]
        df = df.loc[df['1'] != '']
        df['ejercicio_deuda'] = df.loc[df['0'] == '']['1'].str[-4:]
        df["ejercicio_deuda"].fillna(method='bfill', inplace = True)
        df = df.loc[df['7'] != '']
        df = df.loc[df['0'] != 'Entrada']
        df.rename(columns={
            '0':'nro_entrada',
            '1':'nro_origen',
            '2':'fuente',
            '3':'org_fin',
            '4':'importe',
            '5':'saldo',
            '6':'nro_expte',
            '7':'cta_cte',
            '8':'glosa',
        }, inplace=True)
        df['importe'] = df['importe'].str.replace('.', '', regex=False)
        df['importe'] = df['importe'].str.replace(',', '.', regex=False)
        df['importe'] = df['importe'].astype(float)
        df['saldo'] = df['saldo'].str.replace('.', '', regex=False)
        df['saldo'] = df['saldo'].str.replace(',', '.', regex=False)
        df['saldo'] = df['saldo'].astype(float)
        df = df >>\
            dplyr.select(
                f.ejercicio, f.mes_hasta, f.entidad, f.ejercicio_deuda,
                f.fuente, f.nro_entrada, f.nro_origen, f.importe, f.saldo,
                dplyr.everything()
            )

        self.df = pd.DataFrame(df)
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rdeu012b2_c report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "pdf_file or csv_file",
        default='202212-rdeu012b2_C.csv',
        type=str,
        help = "SIIF' rdeu012b2_C report. Must be in the same folder. CSV extension for Windows")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rdeu012 = DeudaFlotanteRdeu012b2C()
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
    # python -m invicodatpy.siif.deuda_flotante_rdeu012b2_c -f 202212-rdeu012b2_C.csv
