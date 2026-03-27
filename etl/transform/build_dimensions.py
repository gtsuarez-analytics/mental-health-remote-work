"""
================================================================================
CONSTRUCCIÓN DE DIMENSIONES — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo de transformación: construye las tablas dimensionales del star schema
a partir de los datasets crudos limpios.

Dimensiones generadas:
    - dim_tiempo: Tiempo (año, mes, trimestre, semestre)
    - dim_trabajo: Modalidad, industria, rol, tamaño de empresa
    - dim_demografia: Género, rango etario, región, país
    - dim_soporte: Tipo y disponibilidad de soporte mental

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import hashlib
from typing import Dict, List
import pandas as pd
from loguru import logger


def _normalizar_genero(valor: str) -> str:
    """
    Normaliza valores de género a categorías estándar.

    Args:
        valor: Valor original del campo género.

    Returns:
        Género normalizado: 'Male', 'Female' u 'Other'.
    """
    if pd.isna(valor):
        return 'Other'

    valor_lower = str(valor).strip().lower()

    if valor_lower in ['male', 'm', 'man', 'masculino']:
        return 'Male'
    elif valor_lower in ['female', 'f', 'woman', 'femenino']:
        return 'Female'
    else:
        return 'Other'


def _clasificar_rango_etario(edad) -> str:
    """
    Clasifica una edad en un rango etario estándar.

    Args:
        edad: Edad como número.

    Returns:
        Rango etario: '18-25', '26-35', '36-45', '46-55', '56-65' u '65+'.
    """
    if pd.isna(edad):
        return 'Unknown'

    try:
        edad_num = int(float(edad))
    except (ValueError, TypeError):
        return 'Unknown'

    if edad_num < 18:
        return 'Under 18'
    elif edad_num <= 25:
        return '18-25'
    elif edad_num <= 35:
        return '26-35'
    elif edad_num <= 45:
        return '36-45'
    elif edad_num <= 55:
        return '46-55'
    elif edad_num <= 65:
        return '56-65'
    else:
        return '65+'


def _clasificar_empresa_tamano(tamano: str) -> str:
    """
    Clasifica el tamaño de empresa en categorías estándar.

    Args:
        tamano: Valor original del tamaño de empresa.

    Returns:
        Tamaño normalizado.
    """
    if pd.isna(tamano):
        return 'Unknown'

    tamano_str = str(tamano).strip().lower()

    if '1-5' in tamano_str or 'micro' in tamano_str:
        return 'Micro (1-5)'
    elif '6-25' in tamano_str or 'small' in tamano_str:
        return 'Small (6-25)'
    elif '26-100' in tamano_str or 'medium' in tamano_str:
        return 'Medium (26-100)'
    elif '100-500' in tamano_str:
        return 'Large (100-500)'
    elif '500-1000' in tamano_str:
        return 'Enterprise (500-1000)'
    elif '1000+' in tamano_str or '1000-' in tamano_str or 'corporate' in tamano_str:
        return 'Corporate (1000+)'
    else:
        return 'Unknown'


def construir_dim_tiempo(anno: int = 2024) -> pd.DataFrame:
    """
    Construye la dimensión de tiempo para el año especificado.

    Args:
        anno: Año para generar la dimensión de tiempo.

    Returns:
        DataFrame con la dimensión de tiempo.
    """
    logger.info(f"Construyendo dim_tiempo para el año {anno}")

    registros = []
    for mes in range(1, 13):
        trimestre = (mes - 1) // 3 + 1
        semestre = 1 if mes <= 6 else 2
        registros.append({
            'id_tiempo': (anno - 2020) * 12 + mes,
            'anno': anno,
            'mes': mes,
            'trimestre': trimestre,
            'semestre': semestre,
        })

    df = pd.DataFrame(registros)
    logger.info(f"dim_tiempo creada: {len(df)} registros")
    return df


def construir_dim_trabajo(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la dimensión de trabajo a partir de los datos crudos.

    Args:
        df_raw: DataFrame con los datos crudos de trabajo remoto.

    Returns:
        DataFrame con la dimensión de trabajo.
    """
    logger.info("Construyendo dim_trabajo")

    # Mapeo de columnas del dataset a la dimensión
    df = pd.DataFrame()

    # Modalidad
    if 'Work_Location' in df_raw.columns:
        df['modalidad'] = df_raw['Work_Location'].fillna('Unknown')
    elif 'remote_work' in df_raw.columns:
        df['modalidad'] = df_raw['remote_work'].map(
            {0: 'In-Person', 1: 'Remote', 'No': 'In-Person', 'Yes': 'Remote'}
        ).fillna('Unknown')
    else:
        df['modalidad'] = 'Unknown'

    # Industria
    if 'Industry' in df_raw.columns:
        df['industria'] = df_raw['Industry'].fillna('Unknown')
    elif 'tech_company' in df_raw.columns:
        df['industria'] = df_raw['tech_company'].map(
            {0: 'Non-Tech', 1: 'Tech', 'No': 'Non-Tech', 'Yes': 'Tech'}
        ).fillna('Unknown')
    else:
        df['industria'] = 'Unknown'

    # Rol (empleado por posición)
    if 'Employment_Status' in df_raw.columns:
        df['rol'] = df_raw['Employment_Status'].fillna('Unknown')
    elif 'job_title' in df_raw.columns:
        df['rol'] = df_raw['job_title'].fillna('Unknown')
    else:
        df['rol'] = 'Unknown'

    # Tamaño de empresa
    if 'Company_Size' in df_raw.columns:
        df['empresa_tamano'] = df_raw['Company_Size'].apply(_clasificar_empresa_tamano)
    elif 'no_employees' in df_raw.columns:
        df['empresa_tamano'] = df_raw['no_employees'].apply(_clasificar_empresa_tamano)
    else:
        df['empresa_tamano'] = 'Unknown'

    # Horas semanales
    if 'Hours_Worked_Per_Week' in df_raw.columns:
        df['horas_semanales'] = pd.to_numeric(df_raw['Hours_Worked_Per_Week'], errors='coerce')
    else:
        df['horas_semanales'] = None

    # Eliminar duplicados
    df = df.drop_duplicates().reset_index(drop=True)

    # Agregar ID
    df.insert(0, 'id_trabajo', range(1, len(df) + 1))

    logger.info(f"dim_trabajo creada: {len(df)} registros")
    return df


def construir_dim_demografia(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la dimensión de demografía a partir de los datos crudos.

    Args:
        df_raw: DataFrame con los datos crudos.

    Returns:
        DataFrame con la dimensión de demografía.
    """
    logger.info("Construyendo dim_demografia")

    df = pd.DataFrame()

    # Género
    if 'Gender' in df_raw.columns:
        df['genero'] = df_raw['Gender'].apply(_normalizar_genero)
    else:
        df['genero'] = 'Other'

    # Rango etario
    if 'Age' in df_raw.columns:
        df['rango_etario'] = df_raw['Age'].apply(_clasificar_rango_etario)
    else:
        df['rango_etario'] = 'Unknown'

    # Región y país - detectar qué columna existe
    if 'Country' in df_raw.columns:
        # Dataset tech survey: usa Country y state
        df['pais'] = df_raw['Country'].fillna('Unknown')
        if 'state' in df_raw.columns:
            df['region'] = df_raw['state'].fillna('Unknown')
        else:
            df['region'] = 'Unknown'
    elif 'Region' in df_raw.columns:
        # Dataset remote work: usa Region
        df['region'] = df_raw['Region'].fillna('Unknown')
        df['pais'] = 'Unknown'
    else:
        df['region'] = 'Unknown'
        df['pais'] = 'Unknown'

    # Eliminar duplicados
    df = df.drop_duplicates().reset_index(drop=True)

    # Agregar ID
    df.insert(0, 'id_demografia', range(1, len(df) + 1))

    logger.info(f"dim_demografia creada: {len(df)} registros")
    return df


def construir_dim_soporte() -> pd.DataFrame:
    """
    Construye la dimensión de soporte de salud mental.

    Returns:
        DataFrame con la dimensión de soporte.
    """
    logger.info("Construyendo dim_soporte")

    registros = [
        {'id_soporte': 1, 'tipo_soporte': 'Employee Assistance Program', 'disponible': True},
        {'id_soporte': 2, 'tipo_soporte': 'Counseling Services', 'disponible': True},
        {'id_soporte': 3, 'tipo_soporte': 'Mental Health Days', 'disponible': True},
        {'id_soporte': 4, 'tipo_soporte': 'Wellness Workshops', 'disponible': True},
        {'id_soporte': 5, 'tipo_soporte': 'No Support Available', 'disponible': False},
    ]

    df = pd.DataFrame(registros)
    logger.info(f"dim_soporte creada: {len(df)} registros")
    return df
