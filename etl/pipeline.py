"""
================================================================================
PIPELINE ETL — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Orquestador principal del pipeline de Extracción, Transformación y Carga (ETL).

Ejecuta las siguientes fases en orden:
    1. Extracción: Carga datasets crudos de Kaggle
    2. Transformación: Construye dimensiones y tabla de hechos
    3. Carga: Inserta datos en PostgreSQL y guarda CSVs procesados

CONFIGURACIÓN:
    - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD en .env
    - DATA_RAW_PATH, DATA_PROCESSED_PATH en .env

Uso:
    python etl/pipeline.py

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import sys
from pathlib import Path
from datetime import datetime

# Agregar raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from loguru import logger
import pandas as pd

from etl.utils.logger_config import configurar_logger
from etl.utils.db_connector import obtener_engine, verificar_conexion
from etl.extract.extract_datasets import ejecutar_extraccion
from etl.transform.build_dimensions import (
    construir_dim_tiempo,
    construir_dim_trabajo,
    construir_dim_demografia,
    construir_dim_soporte,
)
from etl.transform.build_fact_table import construir_fact_table
from etl.load.load_to_postgres import cargar_a_postgres

load_dotenv()


def ejecutar_pipeline() -> None:
    """
    Ejecuta el pipeline ETL completo.
    """
    configurar_logger("pipeline")
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE ETL — SALUD MENTAL EN EL TRABAJO REMOTO")
    logger.info(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    try:
        # ── FASE 0: VALIDACIÓN ─────────────────────────────────────────
        logger.info("")
        logger.info(">>> FASE 0: VALIDACIÓN DE DATOS")
        logger.info("-" * 40)

        from etl.utils.validators import ejecutar_validacion
        datasets = ejecutar_extraccion()

        if ejecutar_validacion(datasets.get("remote_work", pd.DataFrame()),
                              datasets.get("tech_survey", pd.DataFrame())):
            logger.info("Validación de datos: APROBADA")
        else:
            logger.warning("Validación de datos: FALLIDA - continuando con precaución")

        # ── FASE 1: EXTRACCIÓN ──────────────────────────────────────────
        logger.info("")
        logger.info(">>> FASE 1: EXTRACCIÓN")
        logger.info("-" * 40)

        df_remote = datasets.get("remote_work")
        if df_remote is None or len(df_remote) == 0:
            logger.error("No se pudieron cargar los datos crudos")
            return

        logger.info("Datasets cargados exitosamente")

        # ── FASE 2: TRANSFORMACIÓN ─────────────────────────────────────
        logger.info("")
        logger.info(">>> FASE 2: TRANSFORMACIÓN")
        logger.info("-" * 40)

        # Construir dimensiones
        dim_tiempo = construir_dim_tiempo(anno=2024)
        dim_trabajo = construir_dim_trabajo(df_remote)
        dim_demografia = construir_dim_demografia(df_remote)
        dim_soporte = construir_dim_soporte()

        # Construir tabla de hechos
        fact_bienestar = construir_fact_table(
            df_remote,
            dim_trabajo,
            dim_demografia,
            dim_tiempo,
            dim_soporte
        )

        datos_transformados = {
            "dim_tiempo": dim_tiempo,
            "dim_trabajo": dim_trabajo,
            "dim_demografia": dim_demografia,
            "dim_soporte": dim_soporte,
            "fact_bienestar_laboral": fact_bienestar,
        }

        # ── FASE 3: CARGA ──────────────────────────────────────────────
        logger.info("")
        logger.info(">>> FASE 3: CARGA")
        logger.info("-" * 40)

        # Intentar conectar a PostgreSQL
        try:
            engine = obtener_engine()

            if verificar_conexion(engine):
                cargar_a_postgres(datos_transformados, engine, guardar_csv=True)
            else:
                logger.warning("No se pudo verificar la conexión a PostgreSQL")
                logger.info("Guardando solo CSVs procesados")

                # Guardar solo CSVs
                from etl.load.load_to_postgres import guardar_csv_procesado
                for nombre, df in datos_transformados.items():
                    guardar_csv_procesado(df, nombre)

        except Exception as e:
            logger.warning(f"Error al conectar a PostgreSQL: {e}")
            logger.info("Guardando datos solo como CSV")

            from etl.load.load_to_postgres import guardar_csv_procesado
            for nombre, df in datos_transformados.items():
                guardar_csv_procesado(df, nombre)

        # ── RESUMEN ───────────────────────────────────────────────────
        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info("Resumen:")
        for nombre, df in datos_transformados.items():
            logger.info(f"  - {nombre}: {len(df)} registros")
        logger.info("")

    except Exception as e:
        logger.error(f"ERROR EN EL PIPELINE: {e}")
        raise


if __name__ == "__main__":
    ejecutar_pipeline()
