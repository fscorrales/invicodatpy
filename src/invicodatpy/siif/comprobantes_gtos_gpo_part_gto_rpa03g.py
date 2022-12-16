#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's gto_rpa03g report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils


class ComprobantesGtosGpoPartGtoRpa03g(RPWUtils):
    """Read, process and write SIIF's gto_rpa03g report"""
    _REPORT_TITLE = 'DETALLE DE DOCUMENTOS ORDENADOS. PARTIDA'
    _TABLE_NAME = 'comprobantes_gtos_gpo_part_gto_rpa03g'
    _INDEX_COL = 'id'
    _FILTER_COL = ['mes', 'grupo']
    _SQL_MODEL = SIIFModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['18'].iloc[5][:40]
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
        df['ejercicio'] = df.iloc[3,18][-4:]
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-21) >> \
            tidyr.drop_na(f['1']) >> \
            dplyr.transmute(
                ejercicio = f.ejercicio,
                nro_entrada = f['1'],
                nro_origen = f['5'],
                importe = base.as_double(f['8']),
                fecha = f['14'],
                partida =  f['17'],
                grupo = f.partida.str[0] + '00',
                nro_expte = f['19'],
                glosa = f['21'],
                beneficiario = f['23'],
                mes =  f.fecha.str[5:7] + '/' + f.ejercicio,
                nro_comprobante = f['nro_entrada'].str.zfill(5) + '/' + f['mes'].str[-2:]
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.nro_comprobante, f.importe,
                f.grupo, f.partida,
                dplyr.everything()
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's gto_rpa03g report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-gto_rpa03g (Gpo 400).xls',
        type=str,
        help = "SIIF' gto_rpa03g.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_gto_rpa03g = ComprobantesGtosGpoPartGtoRpa03g()
    siif_gto_rpa03g.from_external_report(dir_path + '/' + args.file)
    # siif_gto_rpa03g.test_sql(dir_path + '/test.sqlite')
    siif_gto_rpa03g.to_sql(dir_path + '/siif.sqlite')
    siif_gto_rpa03g.print_tidyverse()
    siif_gto_rpa03g.from_sql(dir_path + '/siif.sqlite')
    siif_gto_rpa03g.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.comprobantes_gtos_gpo_part_gto_rpa03g