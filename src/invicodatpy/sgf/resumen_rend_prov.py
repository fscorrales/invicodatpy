#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SGF's 'Resumen de 
Rendiciones por Proveedores' report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import dplyr, f

from .sgf import SGF


class ResumenRendProv(SGF):
    """Read, process and write SGF's 'Resumen de Rendiciones 
    por Proveedores' report"""
    _REPORT_TITLE = 'Resumen de Rendiciones (Detalle)'
    _TABLE_NAME = 'resumen_rend_prov'
    _INDEX_COL = 'id'
    _FILTER_COL = ['origen', 'mes']

    # --------------------------------------------------
    def from_sgf_csv_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = self.read_csv(csv_path, names = list(range(0,70)))
        read_title = df['1'].iloc[0][0:32]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read csv file"""
        df = self.df
        df['origen'] = df['6'].str.split('-', n = 1).str[0]
        df['origen'] = df['origen'].str.split('=', n = 1).str[1]
        df['origen'] = df['origen'].str.replace('"','')
        df['origen'] = df['origen'].str.strip()
        
        if df.loc[0, 'origen'] == 'OBRAS':
            df = df >> \
                dplyr.transmute(
                    origen = f.origen,
                    beneficiario = f['23'], 
                    destino = '', 
                    libramiento_sgf = f['25'], 
                    fecha = f['26'], 
                    movimiento = f['27'],
                    cta_cte = f['24'],
                    importe_bruto = f['28'],
                    gcias = f['29'], 
                    sellos = f['30'],
                    iibb = f['31'], 
                    suss = f['32'], 
                    invico = f['33'], 
                    seguro = '0',
                    salud = '0', 
                    mutual = '0', 
                    otras = f['34'],
                    importe_neto = f['35'], 
                )
        else:
            df = df >> \
                dplyr.transmute(
                    origen = f.origen,
                    beneficiario = f['26'], 
                    destino = f['27'], 
                    libramiento_sgf = f['29'], 
                    fecha = f['30'], 
                    movimiento = f['31'],
                    cta_cte = f['28'],
                    importe_bruto = f['41'], 
                    gcias = f['33'], 
                    sellos = f['34'],
                    iibb = f['35'], 
                    suss = f['36'], 
                    invico = f['37'], 
                    seguro = f['38'],
                    salud = f['39'], 
                    mutual = f['40'], 
                    otras = '0',
                    importe_neto = f['32']
                )
        
        df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        ).str.replace(',','').unstack()
        df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        ).astype(float).unstack()
        df['retenciones'] = df.loc[:,'gcias':'otras'].sum(axis=1)

        df = df >> \
            dplyr.relocate(f.retenciones, _before = f.importe_neto) >> \
            dplyr.mutate(
                ejercicio = f.fecha.str[-4:],
                mes = f.fecha.str[3:5] + '/' + f.ejercicio
            )  >> \
            dplyr.select(
                f.origen, f.ejercicio, f.mes, f.fecha,
                dplyr.everything()
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%d/%m/%Y'
        )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SGF's 'Resumen de " +
        "Rendiciones por Proveedores' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 Resumen de Rendiciones EPAM.csv',
        type=str,
        help = "SGF' Resumen de Rendiciones.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_resumen_rend_prov = ResumenRendProv()
    sgf_resumen_rend_prov.from_sgf_csv_report(dir_path + '/' + args.file)
    # sgf_resumen_rend_prov.test_sql(dir_path + '/test.sqlite')
    sgf_resumen_rend_prov.to_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_prov.print_tidyverse()
    sgf_resumen_rend_prov.from_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_prov.print_tidyverse()
    # print(sgf_resumen_rend_prov.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.resumen_rend_prov -f '2022 Resumen de Rendiciones OBRAS.csv'
