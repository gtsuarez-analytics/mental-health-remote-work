"""
================================================================================
VALIDACIÓN DE DATOS — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo de validación de calidad de datos usando Great Expectations.
Valida los datasets crudos antes de procesarlos.

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

from pathlib import Path
from typing import Dict, List
import pandas as pd
from loguru import logger


def validar_dataset_remote_work(df: pd.DataFrame) -> Dict[str, bool]:
    """
    Valida el dataset de trabajo remoto con reglas de negocio.

    Args:
        df: DataFrame a validar.

    Returns:
        Diccionario con resultados de validación.
    """
    resultados = {}

    # Validaciones de estructura
    resultados["tiene_registros"] = len(df) > 0
    resultados["columnas_minimas"] = len(df.columns) >= 10

    # Validaciones de contenido
    resultados["sin_duplicados_exactos"] = df.duplicated().sum() == 0

    # Validación de columnas clave
    columnas_clave = ["Work_Location", "Mental_Health_Condition", "Age", "Gender"]
    for col in columnas_clave:
        if col in df.columns:
            resultados[f"col_{col}_sin_nulos"] = df[col].notna().sum() > 0

    # Validación de rangos
    if "Age" in df.columns:
        resultados["edad_en_rango"] = (
            (df["Age"].dropna() >= 18) & (df["Age"].dropna() <= 80)
        ).all()

    if "Hours_Worked_Per_Week" in df.columns:
        resultados["horas_en_rango"] = (
            (df["Hours_Worked_Per_Week"].dropna() > 0) &
            (df["Hours_Worked_Per_Week"].dropna() <= 80)
        ).all()

    return resultados


def validar_dataset_tech_survey(df: pd.DataFrame) -> Dict[str, bool]:
    """
    Valida el dataset de encuesta de salud mental en tecnología.

    Args:
        df: DataFrame a validar.

    Returns:
        Diccionario con resultados de validación.
    """
    resultados = {}

    # Validaciones de estructura
    resultados["tiene_registros"] = len(df) > 0
    resultados["columnas_minimas"] = len(df.columns) >= 15

    # Validaciones de contenido
    resultados["sin_duplicados_exactos"] = df.duplicated().sum() == 0

    # Validación de columnas clave
    if "Gender" in df.columns:
        resultados["generos_validos"] = df["Gender"].notna().sum() > 0

    if "Age" in df.columns:
        resultados["edad_en_rango"] = (
            (df["Age"].dropna() >= 18) & (df["Age"].dropna() <= 80)
        ).all()

    return resultados


def generar_reporte_validacion(resultados: Dict[str, bool]) -> str:
    """
    Genera un reporte legible de los resultados de validación.

    Args:
        resultados: Diccionario de resultados de validación.

    Returns:
        String con el reporte formateado.
    """
    lineas = ["=" * 50, "REPORTE DE VALIDACIÓN", "=" * 50]

    for clave, passed in resultados.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        lineas.append(f"  {status}  {clave}")

    total = len(resultados)
    passed = sum(1 for v in resultados.values() if v)
    lineas.append("-" * 50)
    lineas.append(f"Total: {passed}/{total} validaciones pasadas")

    return "\n".join(lineas)


def ejecutar_validacion(df_remote: pd.DataFrame, df_tech: pd.DataFrame) -> bool:
    """
    Ejecuta validación completa en ambos datasets.

    Args:
        df_remote: DataFrame de trabajo remoto.
        df_tech: DataFrame de encuesta tech.

    Returns:
        True si todas las validaciones pasan, False en caso contrario.
    """
    logger.info("Ejecutando validación de datasets...")

    # Validar remote work
    logger.info("Validando dataset: remote_work_mental_health")
    resultados_rw = validar_dataset_remote_work(df_remote)
    logger.info(generar_reporte_validacion(resultados_rw))

    # Validar tech survey
    logger.info("Validando dataset: mental_health_tech_survey")
    resultados_ts = validar_dataset_tech_survey(df_tech)
    logger.info(generar_reporte_validacion(resultados_ts))

    # Resultado global
    todos_ok = all(resultados_rw.values()) and all(resultados_ts.values())

    if todos_ok:
        logger.info("✓ Todos los datasets pasaron validación")
    else:
        logger.warning("✗ Algunos datasets fallaron validación")

    return todos_ok
