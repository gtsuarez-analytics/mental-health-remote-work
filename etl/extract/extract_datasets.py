"""
================================================================================
EXTRACCIÓN DE DATASETS — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo de extracción del pipeline ETL. Lee los datasets crudos de Kaggle
desde el directorio data/raw y realiza una validación básica de estructura.

Datasets manejados:
    - remote_work_mental_health.csv: Datos de trabajo remoto y salud mental
    - mental_health_tech_survey.csv: Encuesta de salud mental en tecnología

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

from pathlib import Path
from typing import Dict
import pandas as pd
from loguru import logger


# Columnas mínimas esperadas en cada dataset
COLUMNAS_REMOTE_WORK = {
    "Employment_Status",
    "Work_Location",
    "Mental_Health_Condition",
    "Stress_Level",
    "Productivity",
    "Work_Life_Balance",
    "Access_to_Mental_Health_Resources",
    "Age",
    "Gender",
    "Years_of_Experience",
    "Industry",
    "Company_Size",
    "Hours_Worked_Per_Week",
}

COLUMNAS_TECH_SURVEY = {
    "Age",
    "Gender",
    "Country",
    "state",
    "self_employed",
    "family_history",
    "treatment",
    "work_interfere",
    "no_employees",
    "remote_work",
    "tech_company",
    "benefits",
    "care_options",
    "wellness_program",
    "seek_help",
    "anonymity",
    "leave",
    "mental_health_consequence",
    "phys_health_consequence",
    "coworkers",
    "supervisor",
    "mental_health_interview",
    "phys_health_interview",
    "mental_vs_physical",
    "obs_consequence",
    "comments",
}


def cargar_dataset_remote_work(ruta_raw: str = "data/raw") -> pd.DataFrame:
    """
    Carga y valida el dataset de trabajo remoto y salud mental.

    Args:
        ruta_raw: Ruta al directorio de datos crudos.

    Returns:
        DataFrame con los datos de trabajo remoto.

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si faltan columnas requeridas.
    """
    ruta = Path(ruta_raw) / "remote_work_mental_health.csv"

    if not ruta.exists():
        logger.error(f"Archivo no encontrado: {ruta}")
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")

    logger.info(f"Cargando dataset: {ruta}")
    df = pd.read_csv(ruta)

    # Validar columnas
    columnas_faltantes = COLUMNAS_REMOTE_WORK - set(df.columns)
    if columnas_faltantes:
        logger.warning(f"Columnas faltantes en remote_work: {columnas_faltantes}")

    logger.info(f"Dataset cargado: {len(df)} filas, {len(df.columns)} columnas")
    return df


def cargar_dataset_tech_survey(ruta_raw: str = "data/raw") -> pd.DataFrame:
    """
    Carga y valida el dataset de encuesta de salud mental en tecnología.

    Args:
        ruta_raw: Ruta al directorio de datos crudos.

    Returns:
        DataFrame con los datos de la encuesta tech.

    Raises:
        FileNotFoundError: Si el archivo no existe.
    """
    ruta = Path(ruta_raw) / "mental_health_tech_survey.csv"

    if not ruta.exists():
        logger.error(f"Archivo no encontrado: {ruta}")
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")

    logger.info(f"Cargando dataset: {ruta}")
    df = pd.read_csv(ruta)

    # Validar columnas
    columnas_faltantes = COLUMNAS_TECH_SURVEY - set(df.columns)
    if columnas_faltantes:
        logger.warning(f"Columnas faltantes en tech_survey: {columnas_faltantes}")

    logger.info(f"Dataset cargado: {len(df)} filas, {len(df.columns)} columnas")
    return df


def ejecutar_extraccion(ruta_raw: str = "data/raw") -> Dict[str, pd.DataFrame]:
    """
    Ejecuta la fase de extracción del pipeline ETL.

    Args:
        ruta_raw: Ruta al directorio de datos crudos.

    Returns:
        Diccionario con los datasets cargados.
    """
    logger.info("=== INICIANDO FASE DE EXTRACCIÓN ===")

    datasets = {}

    try:
        datasets["remote_work"] = cargar_dataset_remote_work(ruta_raw)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error al cargar remote_work: {e}")
        raise

    try:
        datasets["tech_survey"] = cargar_dataset_tech_survey(ruta_raw)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error al cargar tech_survey: {e}")
        raise

    logger.info(
        f"=== EXTRACCIÓN COMPLETADA: {sum(len(d) for d in datasets.values())} "
        f"registros totales ==="
    )

    return datasets
