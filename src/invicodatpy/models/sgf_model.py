#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SGF database model
"""

import os
from dataclasses import dataclass

from sqlalchemy import (Boolean, Column, Date, Integer, MetaData, Numeric,
                        String, Table, create_engine)


@dataclass
class SGFModel():
    sql_path:str

    def __post_init__(self):
        self.metadata = MetaData()
        self.model_tables()
        self.create_engine()
        self.create_database()

    def model_tables(self):
        """Create table models"""
        self.listado_prov = Table(
            'listado_prov', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('codigo', String(5)),
            Column('cuit', String(15)),
            Column('descripcion', String(50)),
            Column('domicilio', String(50)),
            Column('localidad', String(30)),
            Column('telefono', String(30)),
            Column('condicion_iva', String(30))
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)