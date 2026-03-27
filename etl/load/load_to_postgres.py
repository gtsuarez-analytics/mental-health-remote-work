"""
================================================================================
CARGA A POSTGRESQL — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo de carga del pipeline ETL. Inserta los DataFrames transformados
en las tablas del star schema en PostgreSQL usando SQLAlchemy.

Estrategia: replace por tabla (truncate + insert) para idempotencia.
Las dimensiones se cargan antes que la tabla de hechos.

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

from pathlib import Path
from typing import Dict
import pandas as pd
from loguru import logger
from sqlalchemy.engine import Engine
from sqlalchemy import text


ORDEN_CARGA = [
    "dim_tiempo",
    "dim_trabajo",
    "dim_demografia",
    "dim_soporte",
    "fact_bienestar_laboral",
]


def guardar_csv_procesado(df: pd.DataFrame, nombre: str, ruta: str = "data/processed") -> None:
    """
    Guarda un DataFrame como CSV en la carpeta de datos procesados.

    Args:
        df: DataFrame a guardar.
        nombre: Nombre del archivo (sin extensión).
        ruta: Ruta al directorio de datos procesados.
    """
    rutaCarpeta = Path(ruta)
    rutaCarpeta.mkdir(parents=True, exist_ok=True)

    rutaArchivo = rutaCarpeta / f"{nombre}.csv"

    df.to_csv(rutaArchivo, index=False)
    logger.info(f"CSV guardado: {rutaArchivo} ({len(df)} filas)")


def _truncar_tabla(engine: Engine, nombre_tabla: str) -> None:
    """
    Trunca una tabla en la base de datos (borra todos los registros).

    Args:
        engine: Engine de SQLAlchemy.
        nombre_tabla: Nombre de la tabla a truncar.
    """
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {nombre_tabla} RESTART IDENTITY CASCADE"))
        conn.commit()
    logger.info(f"Tabla truncada: {nombre_tabla}")


def cargar_a_postgres(
    datos: Dict[str, pd.DataFrame],
    engine: Engine,
    guardar_csv: bool = True
) -> None:
    """
    Carga todos los DataFrames a PostgreSQL en el orden correcto.

    Args:
        datos: Diccionario con nombres de tablas como claves y DataFrames como valores.
        engine: Engine de SQLAlchemy conectado a PostgreSQL.
        guardar_csv: Si True, también guarda los datos como CSV en processed/.
    """
    logger.info("=== INICIANDO FASE DE CARGA ===")

    for nombre_tabla in ORDEN_CARGA:
        if nombre_tabla not in datos:
            logger.warning(f"Tabla no encontrada en datos: {nombre_tabla}")
            continue

        df = datos[nombre_tabla]

        if df is None or len(df) == 0:
            logger.warning(f"DataFrame vacío para {nombre_tabla}, omitiendo")
            continue

        logger.info(f"Cargando {nombre_tabla}: {len(df)} registros")

        # Guardar CSV si se solicita
        if guardar_csv:
            guardar_csv_procesado(df, nombre_tabla)

        # Truncar tabla
        _truncar_tabla(engine, nombre_tabla)

        # Insertar datos
        try:
            df.to_sql(
                name=nombre_tabla,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )
            logger.info(f"Carga completada: {nombre_tabla}")
        except Exception as e:
            logger.error(f"Error al cargar {nombre_tabla}: {e}")
            raise

    logger.info("=== CARGA COMPLETADA ===")


def verificar_carga(engine: Engine, tabla: str) -> int:
    """
    Verifica que los datos se cargaron correctamente contando registros.

    Args:
        engine: Engine de SQLAlchemy.
        tabla: Nombre de la tabla a verificar.

    Returns:
        Número de registros en la tabla.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}"))
        count = result.scalar()

    logger.info(f"{tabla}: {count} registros")
    return count
