#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SIIF database model
"""

import os
from dataclasses import dataclass

from sqlalchemy import (Column, Integer, MetaData, Numeric, String, Table,
                        create_engine)

@dataclass
class SIIFModel():
    sql_path:str

    def __post_init__(self):
        self.metadata = MetaData()
        self.model_tables()
        self.create_engine()
        self.create_database()

    def model_tables(self):
        """Create table models"""
        self.ppto_gtos_fte_rf602 = Table(
            'ppto_gtos_fte_rf602', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('programa', String(2)),
            Column('subprograma', String(2)),
            Column('proyecto', String(2)),
            Column('actividad', String(2)),
            Column('partida', String(3)),
            Column('grupo', String(3)),
            Column('fuente', String(2)),
            Column('org', String(1)),
            Column('credito_original', Numeric(12,2)),
            Column('credito_vigente', Numeric(12,2)),
            Column('comprometido', Numeric(12,2)),
            Column('ordenado', Numeric(12,2)),
            Column('saldo', Numeric(12,2)),
            Column('pendiente', Numeric(12,2))
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)