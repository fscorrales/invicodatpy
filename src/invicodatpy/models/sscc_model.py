#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SGF database model
"""

import os
from dataclasses import dataclass

from sqlalchemy import (Boolean, Column, Date, Integer, MetaData, Numeric,
                        String, Table, create_engine)


@dataclass
class SSCCModel():
    sql_path:str

    def __post_init__(self):
        if not os.path.exists(self.sql_path):
            self.metadata = MetaData()
            self.model_tables()
            self.create_engine()
            self.create_database()
        else:
            self.create_engine()


    def model_tables(self):
        """Create table models"""

        self.banco_invico = Table(
            'banco_invico', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('cta_cte', String(20)),
            Column('movimiento', String(10)),
            Column('es_cheque', Boolean()),
            Column('beneficiario', String(50)),
            Column('importe', Numeric(12,2)),
            Column('concepto', String(50)),
            Column('moneda', String(15)),
            Column('libramiento', String(10)),
            Column('cod_imputacion', String(3)),
            Column('imputacion', String(50))
        )

        self.sdo_final_banco_invico = Table(
            'sdo_final_banco_invico', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cta_cte', String(20)),
            Column('desc_cta_cte', String(50)),
            Column('desc_banco', String(50)),
            Column('saldo', Numeric(12,2))
        )

        self.ctas_ctes = Table(
            'ctas_ctes', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('map_to', String(20)),
            Column('sscc_cta_cte', String(20)),
            Column('real_cta_cte', String(20)),
            Column('siif_recursos_cta_cte', String(20)),
            Column('siif_gastos_cta_cte', String(20)),
            Column('siif_contabilidad_cta_cte', String(20)),
            Column('sgf_cta_cte', String(20)),
            Column('siif_cta_cte', String(20)),
            Column('icaro_cta_cte', String(20)),
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)