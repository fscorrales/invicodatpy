#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rcg01_uejp report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .siif import SIIF

class ComprobantesGtosRcg01Uejp(SIIF):
    """Read, process and write SIIF's rcg01_uejp report"""
    _REPORT_TITLE = 'Resumen Diario de Comprobantes de Gastos Ingresados'
    _TABLE_NAME = 'comprobantes_gtos_rcg01_uejp'
    _INDEX_COL = 'id'
    _FILTER_COL = 'ejercicio'

    # --------------------------------------------------
    def from_siif_xls_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['1'].iloc[4]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        self.df = self.df.replace(to_replace='', value=None)      
        df = self.df >> \
            dplyr.mutate(
                ejercicio = f['1'].iloc[2][-4:]
            ) >> \
            dplyr.select(~f['0'], ~f['17'], ~f['18']) >> \
            base.tail(-16)

        df.columns = [
            "nro_entrada", "nro_origen", "fuente", "clase_reg",
            "clase_mod", "clase_gto", "fecha", "monto",
            "cuit", "beneficiario", "nro_expte","cta_cte",
            "comprometido", "verificado", "aprobado", "pagado",
            "nro_fondo", "ejercicio"
        ]

        df = df >> \
            tidyr.drop_na(f.cuit) >> \
            tidyr.drop_na(f.nro_entrada) >> \
            dplyr.mutate(
                monto = base.as_double(f.monto),
                beneficiario = f.beneficiario.str.replace("\t", ""),
                comprometido = dplyr.if_else(f.comprometido == "S", True, False),
                verificado = dplyr.if_else(f.verificado == "S", True, False),
                aprobado = dplyr.if_else(f.aprobado == "S", True, False),
                pagado = dplyr.if_else(f.pagado == "S", True, False)
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')
        df['nro_comprobante'] = df['nro_entrada'].str.zfill(5) + '/' + df['mes'].str[-2:]

        df = df >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.nro_comprobante, f.monto,
                f.fuente, f.cta_cte, f.cuit,
                f.nro_expte, f.nro_fondo,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rcg01_uejp report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rcg01_uejp.xls',
        type=str,
        help = "SIIF' rcg01_uejp.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rcg01_uejp = ComprobantesGtosRcg01Uejp()
    siif_rcg01_uejp.from_siif_xls_report(dir_path + '/' + args.file)
    # siif_rcg01_uejp.test_sql(dir_path + '/test.sqlite')
    siif_rcg01_uejp.to_sql(dir_path + '/siif.sqlite')
    siif_rcg01_uejp.print_tidyverse()
    siif_rcg01_uejp.from_sql(dir_path + '/siif.sqlite')
    siif_rcg01_uejp.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.comprobantes_gtos_rcg01_uejp