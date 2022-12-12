#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SGF's 'Informe para Contable' report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .sgf import SGF

class CertificadosObras(SGF):
    """Read, process and write SGF's 'Informe para Contable' report"""
    _REPORT_TITLE = 'Resumen de Certificaciones: '
    _TABLE_NAME = 'certificados_obras'
    _INDEX_COL = 'id'
    _FILTER_COL = 'ejercicio'

    # --------------------------------------------------
    def from_sgf_csv_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = self.read_csv(csv_path, names = list(range(0,70)))
        read_title = df['1'].iloc[0]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read csv file"""
        self.df = self.df >> \
            dplyr.transmute(
                ejercicio = f['2'].iloc[0][-4:],
                beneficiario = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['21']
                ),
                obra = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['22'],
                    True, f['21']
                ),
                nro_certificado = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['23'],
                    True, f['22']
                ),
                monto_certificado = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['26'],
                    True, f['25']
                ),
                fondo_reparo = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['27'],
                    True, f['26']
                ),
                otros = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['28'],
                    True, f['27']
                ),
                importe_bruto = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['29'],
                    True, f['28']
                ),
                iibb = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['30'],
                    True, f['29']
                ),
                lp = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['31'],
                    True, f['30']
                ),
                suss = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['32'],
                    True, f['31']
                ),
                gcias = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['33'],
                    True, f['32']
                ),
                invico = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['34'],
                    True, f['33']
                ),
                retenciones = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['35'],
                    True, f['34']
                ),
                importe_neto = dplyr.case_when(
                    (f['37'] == "TOTALES") | (f['48'] == "TOTALES"), f['36'],
                    True, f['35']
                ) 
            ) >> \
            dplyr.mutate(
                monto_certificado = base.gsub(',', '', f.monto_certificado),
                fondo_reparo = base.gsub(',', '', f.fondo_reparo),
                otros = base.gsub(',', '', f.otros),
                importe_bruto = base.gsub(',', '', f.importe_bruto),
                iibb = base.gsub(',', '', f.iibb),
                lp = base.gsub(',', '', f.lp),
                suss = base.gsub(',', '', f.suss),
                gcias = base.gsub(',', '', f.gcias),
                invico = base.gsub(',', '', f.invico),
                retenciones = base.gsub(',', '', f.retenciones),
                importe_neto = base.gsub(',', '', f.importe_neto)
            ) >> \
            dplyr.mutate(
                dplyr.across(base.c[f.monto_certificado:], base.as_double)
            ) >> \
            tidyr.separate(
                f.obra, 
                into = ['cod_obra', None], 
                sep= ' ' ,remove=False, extra='merge'
            ) >> \
            tidyr.fill(f.beneficiario) >> \
            dplyr.select(
                f.ejercicio, f.beneficiario,
                f.cod_obra, f.obra,
                dplyr.everything()
            )

        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SGF's 'Informe para Contable' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 Informe para Contable.csv',
        type=str,
        help = "SGF' Informe para Contable.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_informe_contable = CertificadosObras()
    sgf_informe_contable.from_sgf_csv_report(dir_path + '/' + args.file)
    # sgf_informe_contable.test_sql(dir_path + '/test.sqlite')
    sgf_informe_contable.to_sql(dir_path + '/sgf.sqlite')
    sgf_informe_contable.print_tidyverse()
    sgf_informe_contable.from_sql(dir_path + '/sgf.sqlite')
    sgf_informe_contable.print_tidyverse()
    # print(siif_rf610.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.certificados_obras
