#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)
"""

import argparse
import inspect
import os

import pandas as pd

from ..utils.print_tidyverse import PrintTidyverse
from .comprobantes_gtos_gpo_part_gto_rpa03g import ComprobantesGtosGpoPartGtoRpa03g
from .comprobantes_gtos_rcg01_uejp import ComprobantesGtosRcg01Uejp
from .detalle_partidas_rog01 import DetallePartidasRog01


class JoinComprobantesGtosGpoPart():
    """Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)"""
    df:pd.DataFrame = None
    
    # --------------------------------------------------
    def from_external_report(
        self, gtos_gpo_part_xls_path:str, gtos_xls_path:str, part_xlx_path:str
    ) -> pd.DataFrame:
        self.df_gtos_gpo_part = ComprobantesGtosGpoPartGtoRpa03g().from_external_report(gtos_gpo_part_xls_path)
        self.df_gtos = ComprobantesGtosRcg01Uejp().from_external_report(gtos_xls_path)
        self.df_part = DetallePartidasRog01().from_external_report(part_xlx_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def from_sql(self, sql_path:str) -> pd.DataFrame:
        self.df_gtos_gpo_part = ComprobantesGtosGpoPartGtoRpa03g().from_sql(sql_path)
        self.df_gtos = ComprobantesGtosRcg01Uejp().from_sql(sql_path)
        self.df_part = DetallePartidasRog01().from_sql(sql_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def join_df(self) -> pd.DataFrame:
        df_gtos_filtered = self.df_gtos[[
            'nro_comprobante', 'nro_fondo', 'fuente', 'cta_cte',
            'cuit', 'clase_reg', 'clase_mod', 'clase_gto',
            'es_comprometido', 'es_verificado', 'es_aprobado',
            'es_pagado'
        ]]
        self.df = pd.merge(
            left=self.df_gtos_gpo_part,
            right=df_gtos_filtered,
            on=['nro_comprobante'],
            how='left'
        )
        self.df = pd.merge(
            left=self.df,
            right=self.df_part,
            on=['partida'],
            how='left'
        )
        return self.df

    # --------------------------------------------------
    def print_tidyverse(self):
        print(PrintTidyverse(self.df))

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--sql_file', 
        metavar = "sql_file",
        default='siif.sqlite',
        type=str,
        help = "SIIF' sqlite DataBase file name. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_join_comprobantes_gtos = JoinComprobantesGtosGpoPart()
    siif_join_comprobantes_gtos.from_sql(dir_path + '/' + args.sql_file)
    siif_join_comprobantes_gtos.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.join_comprobantes_gtos_gpo_part -f siif.sqlite