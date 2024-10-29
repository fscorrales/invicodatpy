#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Slave database model
"""

__all__ = ['SlaveModel']

import os
from dataclasses import dataclass

from sqlalchemy import (Boolean, Column, Date, ForeignKey, Integer, MetaData,
                        Numeric, String, Table, create_engine)


@dataclass
class SlaveModel():
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

        self.factureros = Table(
            'factureros', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('razon_social', String(50), unique = True, nullable = False),
            Column('actividad', String(11)),
            Column('partida', String(3))
        )

        self.honorarios_factureros = Table(
            'honorarios_factureros', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('nro_comprobante', String(8)),
            Column('tipo', String(1)),
            Column('razon_social', String(50), nullable = False),
            Column('actividad', String(11)),
            Column('partida', String(3)),
            Column('importe_bruto', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('lp', Numeric(12,2)),
            Column('sellos', Numeric(12,2)),
            Column('seguro', Numeric(12,2)),
            Column('otras_retenciones', Numeric(12,2)),
            Column('anticipo', Numeric(12,2)),
            Column('descuento', Numeric(12,2)),
            Column('mutual', Numeric(12,2), default=0),
            Column('embargo', Numeric(12,2),  default=0),
        )

        # self.comprobantes_siif = Table(
        #     'comprobantes_siif', self.metadata,
        #     Column('nro_comprobante', String(8), primary_key=True, 
        #     unique = True, nullable = False),
        #     Column('fecha', Date()),
        #     Column('tipo', String(1))
        # )

        # self.honorarios_factureros = Table(
        #     'honorarios_factureros', self.metadata,
        #     Column('id', Integer(), autoincrement=True, primary_key=True),
        #     Column('nro_comprobante', ForeignKey('comprobantes_siif.nro_comprobante', 
        #     onupdate="CASCADE", ondelete="CASCADE"), nullable =False),
        #     Column('razon_social', String(50), nullable = False),
        #     Column('actividad', String(11)),
        #     Column('partida', String(3)),
        #     Column('importe_bruto', Numeric(12,2)),
        #     Column('iibb', Numeric(12,2)),
        #     Column('lp', Numeric(12,2)),
        #     Column('sellos', Numeric(12,2)),
        #     Column('seguro', Numeric(12,2)),
        #     Column('otras_retenciones', Numeric(12,2)),
        #     Column('anticipo', Numeric(12,2)),
        #     Column('descuento', Numeric(12,2)),
        #     Column('mutual', Numeric(12,2), default=0),
        #     Column('embargo', Numeric(12,2),  default=0),
        # )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)