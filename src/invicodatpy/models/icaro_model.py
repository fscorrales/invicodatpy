#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Icaro database model
"""

__all__ = ['IcaroModel']

import os
from dataclasses import dataclass

from sqlalchemy import (Boolean, Column, Date, ForeignKey, Integer, MetaData,
                        Numeric, String, Table, create_engine)


@dataclass
class IcaroModel():
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

        self.programas = Table(
            'programas', self.metadata,
            Column('programa', String(2), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_prog', String(100))
        )

        self.subprogramas = Table(
            'subprogramas', self.metadata,
            Column('subprograma', String(5), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_subprog', String(100)),
            Column('programa', ForeignKey('programas.programa', 
            onupdate="CASCADE", ondelete="CASCADE"), nullable =False)
        )

        self.proyectos = Table(
            'proyectos', self.metadata,
            Column('proyecto', String(8), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_proy', String(100)),
            Column('subprograma', ForeignKey('subprogramas.subprograma', 
            onupdate="CASCADE", ondelete="CASCADE"), nullable =False)
        )

        self.actividades = Table(
            'actividades', self.metadata,
            Column('actividad', String(11), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_act', String(100)),
            Column('proyecto', ForeignKey('proyectos.proyecto', 
            onupdate="CASCADE", ondelete="CASCADE"), nullable =False)
        )

        self.ctas_ctes = Table(
            'ctas_ctes', self.metadata,
            Column('cta_cte', String(30), primary_key=True, 
            unique = True, nullable = False),
            Column('cta_cte_anterior', String(30)),
            Column('desc_cta_cte', String(100)),
            Column('banco', String(50))
        )

        self.fuentes = Table(
            'fuentes', self.metadata,
            Column('fuente', String(2), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_fte', String(50)),
            Column('abreviatura', String(10))
        )

        self.partidas = Table(
            'partidas', self.metadata,
            Column('partida', String(3), primary_key=True, 
            unique = True, nullable = False),
            Column('desc_part', String(60)),
            Column('partida_parcial', String(3)),
            Column('desc_part_parcial', String(60)),
            Column('grupo', String(3)),
            Column('desc_grupo', String(60)),
        )

        self.proveedores = Table(
            'proveedores', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('codigo', String(5)),
            Column('cuit', String(15)),
            Column('desc_prov', String(50)),
            Column('domicilio', String(50)),
            Column('localidad', String(30)),
            Column('telefono', String(30)),
            Column('condicion_iva', String(30))
        )

        self.obras = Table(
            'obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('obra', String(150), index = True, unique = True, 
            nullable = False),
            Column('cuit', String(11), nullable = False),
            Column('actividad', String(11), nullable = False),
            Column('partida', String(3), nullable = False),
            Column('fuente', String(2)),
            Column('cta_cte', String(30)),
            Column('localidad', String(30)),
            Column('norma_legal', String(30)),
            Column('info_adicional', String(30)),
            Column('monto_contrato', Numeric(12,2)),
            Column('adicional', Numeric(12,2))
        )

        self.carga = Table(
            'carga', self.metadata,
            Column('id', String(9), primary_key=True, unique = True, 
            nullable = False),
            Column('nro_comprobante', String(8), nullable = False),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date(), nullable = False),
            Column('cuit', String(11), nullable = False),
            Column('obra', ForeignKey('obras.obra', 
            onupdate="CASCADE", ondelete="CASCADE"), nullable = False),
            Column('fuente', String(2), nullable = False),
            Column('cta_cte', String(30), nullable = False),
            Column('actividad', String(11)),
            Column('partida', String(3)),
            Column('importe', Numeric(12,2)),
            Column('fondo_reparo', Numeric(12,2)),
            Column('certificado', String(10)),
            Column('avance', Numeric(1,4)),
            Column('origen', String(5)),
            Column('tipo', String(3)),
        )

        self.retenciones = Table(
            'retenciones', self.metadata,
            Column('id', Integer(), primary_key=True, autoincrement=True),
            Column('id_carga', ForeignKey('carga.id', 
            onupdate="CASCADE", ondelete="CASCADE"), nullable = False),
            Column('codigo', String(3), nullable = False),
            Column('importe', Numeric(12,2)),
        )

        self.certificados_obras = Table(
            'certificados_obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('id_carga', ForeignKey('carga.id', 
            onupdate="CASCADE", ondelete="SET NULL"), nullable = True),
            Column('origen', String(5)),
            Column('ejercicio', String(4)),
            Column('beneficiario', String(50)),
            Column('cod_obra', String(10)),
            Column('obra', String(100)),
            Column('nro_certificado', String(6)),
            Column('monto_certificado', Numeric(12,2)),
            Column('fondo_reparo', Numeric(12,2)),
            Column('otros', Numeric(12,2)),
            Column('importe_bruto', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('lp', Numeric(12,2)),
            Column('suss', Numeric(12,2)),
            Column('gcias', Numeric(12,2)),
            Column('invico', Numeric(12,2)),
            Column('importe_neto', Numeric(12,2))
        )

        self.resumen_rend_obras = Table(
            'resumen_rend_obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('id_carga', ForeignKey('carga.id', 
            onupdate="CASCADE", ondelete="SET NULL"), nullable = True),
            Column('origen', String(10)),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('beneficiario', String(50)),
            Column('cod_obra', String(15)),
            Column('obra', String(100)),
            Column('destino', String(30)),
            Column('libramiento_sgf', String(10)),
            Column('movimiento', String(10)),
            Column('importe_bruto', Numeric(12,2)),
            Column('gcias', Numeric(12,2)),
            Column('sellos', Numeric(12,2)),
            Column('lp', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('suss', Numeric(12,2)),
            Column('seguro', Numeric(12,2)),
            Column('salud', Numeric(12,2)),
            Column('mutual', Numeric(12,2)),
            Column('importe_neto', Numeric(12,2))
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)