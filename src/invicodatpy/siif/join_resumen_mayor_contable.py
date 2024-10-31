#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Join rvicon03 (resumen_contable) with rcocc31 (mayor_contable)
"""

__all__ = ["JoinResumenMayorContable"]

import argparse
import datetime as dt
import inspect
import json
import os

import pandas as pd

from ..utils import PrintTidyverse
from .connect_siif import ConnectSIIF
from .mayor_contable_rcocc31 import MayorContableRcocc31
from .resumen_contable_cta_rvicon03 import ResumenContableCtaRvicon03
from dataclasses import dataclass, field


@dataclass
class JoinResumenMayorContable(ConnectSIIF):
    """Join rvicon03 (resumen_contable) with rcocc31 (mayor_contable)"""

    df: pd.DataFrame = field(init=False, repr=False, default=None)

    # --------------------------------------------------
    def download_and_unite_reports(
        self, dir_path: str, ejercicios: list = str(dt.datetime.now().year)
    ):
        rvicon03 = ResumenContableCtaRvicon03()
        rcocc31 = MayorContableRcocc31()

        rvicon03.download_report(dir_path=dir_path, ejercicios=ejercicios)

        if not isinstance(ejercicios, list):
            ejercicios = [ejercicios]
        for ejercicio in ejercicios:
            filename = ejercicio + "-rvicon03.xls"
            df = rvicon03.from_external_report(os.path.join(dir_path, filename))
            print(df["cta_contable"].values.tolist())
            rcocc31.download_report(
                dir_path,
                ejercicios=ejercicio,
                ctas_contables=df["cta_contable"].values.tolist(),
            )

    # --------------------------------------------------
    def from_external_report(
        self, resumen_xls_path: str, mayor_xls_path: str
    ) -> pd.DataFrame:
        self.df_resumen = ResumenContableCtaRvicon03().from_external_report(
            resumen_xls_path
        )
        self.df_mayor = MayorContableRcocc31().from_external_report(mayor_xls_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def from_sql(self, sql_path: str) -> pd.DataFrame:
        self.df_resumen = ResumenContableCtaRvicon03().from_sql(sql_path)
        self.df_mayor = MayorContableRcocc31().from_sql(sql_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def join_df(self) -> pd.DataFrame:
        pass
        # df_ppto_desc_filtered = self.df_ppto_desc[[
        #     'ejercicio', 'estructura',
        #     'desc_prog', 'desc_subprog', 'desc_proy',
        #     'desc_act', "desc_gpo", "desc_part"
        # ]]
        # self.df = pd.merge(
        #     left=self.df_ppto_fte,
        #     right=df_ppto_desc_filtered,
        #     on=['ejercicio', 'estructura'],
        #     how='left'
        # )
        # return self.df

    # --------------------------------------------------
    def print_tidyverse(self):
        print(PrintTidyverse(self.df))


# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description="Join rvicon03 (resumen_contable) with rcocc31 (mayor_contable)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--download", action="store_true")
    parser.add_argument("--no-download", dest="download", action="store_false")
    parser.set_defaults(download=True)

    parser.add_argument(
        "-u",
        "--username",
        metavar="Username",
        default="",
        type=str,
        help="Username to log in SIIF",
    )

    parser.add_argument(
        "-p",
        "--password",
        metavar="Password",
        default="",
        type=str,
        help="Password to log in SIIF",
    )

    parser.add_argument(
        "-e",
        "--ejercicio",
        metavar="Ejercicio",
        default="2024",
        type=str,
        help="Ejercicio to download from SIIF",
    )

    parser.add_argument(
        "-f",
        "--sql_file",
        metavar="sql_file",
        default="siif.sqlite",
        type=str,
        help="SIIF' sqlite DataBase file name. Must be in the same folder",
    )

    return parser.parse_args()


# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    if args.download:
        json_path = dir_path + "/siif_credentials.json"
        if args.username != "" and args.password != "":
            ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    ConnectSIIF(
                        data_json["username"], data_json["password"]
                    )
                json_file.close()
        siif = JoinResumenMayorContable()
        siif.go_to_reports()
        siif.download_and_unite_reports(dir_path=dir_path, ejercicios=args.ejercicio)
        siif.disconnect()
    else:
        print(dir_path)
        siif = JoinResumenMayorContable()


# --------------------------------------------------
if __name__ == "__main__":
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.join_resumen_mayor_contable -e 2024 --no-download
