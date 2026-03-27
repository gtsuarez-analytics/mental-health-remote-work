"""
================================================================================
CONSTRUCCIÓN DE TABLA DE HECHOS — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo de transformación: construye la tabla de hechos fact_bienestar_laboral
uniendo los datos crudos con las claves de las dimensiones y calculando KPIs.

KPI calculado:
    indice_bienestar = (6 - nivel_estres + balance_work_life + productividad) / 3 * 2

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import hashlib
import numpy as np
import pandas as pd
from loguru import logger


def _anonimizar_id(valor: str) -> str:
    """
    Anonimiza un identificador usando hash SHA-256.

    Args:
        valor: Valor original a anonimizar.

    Returns:
        Hash SHA-256 del valor en hexadecimal.
    """
    return hashlib.sha256(str(valor).encode()).hexdigest()[:16]


def _calcular_indice_bienestar(
    nivel_estres: int,
    balance: int,
    productividad: int
) -> float:
    """
    Calcula el índice compuesto de bienestar laboral.

    Formula: (6 - nivel_estres + balance + productividad) / 3 * 2
    Escala resultante: 1-10

    Args:
        nivel_estres: Nivel de estrés (1-5, donde 5 es más estrés).
        balance: Balance vida-trabajo (1-5).
        productividad: Productividad percibida (1-5).

    Returns:
        Índice de bienestar en escala 1-10.
    """
    indice = (6 - nivel_estres + balance + productividad) / 3 * 2
    return round(max(1.0, min(10.0, indice)), 2)


def _mapear_nivel_estres(valor) -> int:
    """
    Mapea valores de estrés a escala numérica 1-5.

    Args:
        valor: Valor original del campo estrés.

    Returns:
        Nivel de estrés en escala 1-5.
    """
    if pd.isna(valor):
        return 3

    valor_str = str(valor).strip().lower()

    # Mapa de valores comunes — match exacto para evitar "unknown" matcheando "no"
    mapa = [
        ('lowest', 1), ('none', 1), ('no', 1),
        ('low', 1),
        ('medium', 3), ('moderate', 3), ('mid', 3),
        ('high', 5),
        ('highest', 5), ('extreme', 5),
    ]

    valor_lower = valor_str.lower()
    for clave, valor_num in mapa:
        if valor_lower == clave:
            return valor_num

    try:
        val = int(float(valor))
        return max(1, min(5, val))
    except (ValueError, TypeError):
        return 3


def _mapear_productividad(valor) -> int:
    """
    Mapea valores de productividad a escala numérica 1-5.

    Args:
        valor: Valor original del campo productividad.

    Returns:
        Nivel de productividad en escala 1-5.
    """
    if pd.isna(valor):
        return 3

    try:
        val = int(float(valor))
        return max(1, min(5, val))
    except (ValueError, TypeError):
        pass

    valor_str = str(valor).strip().lower()

    if any(x in valor_str for x in ['very low', 'poor', '1']):
        return 1
    elif any(x in valor_str for x in ['low', '2']):
        return 2
    elif any(x in valor_str for x in ['medium', 'moderate', 'average', '3']):
        return 3
    elif any(x in valor_str for x in ['high', 'good', '4']):
        return 4
    elif any(x in valor_str for x in ['very high', 'excellent', 'very good', '5']):
        return 5

    return 3


def _mapear_balance(valor) -> int:
    """
    Mapea valores de balance vida-trabajo a escala numérica 1-5.

    Args:
        valor: Valor original del campo balance.

    Returns:
        Nivel de balance en escala 1-5.
    """
    if pd.isna(valor):
        return 3

    try:
        val = int(float(valor))
        return max(1, min(5, val))
    except (ValueError, TypeError):
        pass

    valor_str = str(valor).strip().lower()

    if any(x in valor_str for x in ['very poor', 'very bad', '1']):
        return 1
    elif any(x in valor_str for x in ['poor', 'bad', '2']):
        return 2
    elif any(x in valor_str for x in ['neutral', 'average', '3']):
        return 3
    elif any(x in valor_str for x in ['good', 'well', '4']):
        return 4
    elif any(x in valor_str for x in ['very good', 'excellent', 'very well', '5']):
        return 5

    return 3


def construir_fact_table(
    df_raw: pd.DataFrame,
    dim_trabajo: pd.DataFrame,
    dim_demografia: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
    dim_soporte: pd.DataFrame
) -> pd.DataFrame:
    """
    Construye la tabla de hechos combinando datos crudos con dimensiones.

    Args:
        df_raw: DataFrame con los datos crudos.
        dim_trabajo: Dimensión de trabajo.
        dim_demografia: Dimensión de demografía.
        dim_tiempo: Dimensión de tiempo.
        dim_soporte: Dimensión de soporte.

    Returns:
        DataFrame con la tabla de hechos.
    """
    logger.info("Construyendo fact_bienestar_laboral")

    hechos = []

    for idx, row in df_raw.iterrows():
        # Anonimizar ID de empleado
        id_empleado = _anonimizar_id(f"{idx}_{row.get('Age', idx)}")

        # Obtener claves de dimensiones
        # Mapeo: columna_dimension -> columna_dataset
        id_trabajo = _buscar_clave_dim(
            dim_trabajo, row,
            ['modalidad', 'industria', 'rol', 'empresa_tamano'],
            mapeo_campos={'modalidad': 'Work_Location', 'industria': 'Industry'}
        )
        id_demografia = _buscar_clave_dim(
            dim_demografia, row,
            ['genero', 'rango_etario', 'region', 'pais'],
            mapeo_campos={'genero': 'Gender', 'region': 'Region'}
        )
        id_tiempo = dim_tiempo['id_tiempo'].iloc[0]  # Usa el primer mes por defecto
        id_soporte = _determinar_soporte(row, dim_soporte)

        # Extraer y mapear métricas
        nivel_estres = _mapear_nivel_estres(
            row.get('Stress_Level') or row.get('work_interfere')
        )
        balance = _mapear_balance(
            row.get('Work_Life_Balance') or row.get('no_employees')
        )
        productividad = _mapear_productividad(row.get('Productivity'))

        # Calcular índice de bienestar
        indice_bienestar = _calcular_indice_bienestar(nivel_estres, balance, productividad)

        # Determinar acceso a soporte
        accede_soporte = _verificar_soporte(row)

        # Fecha de registro (si existe)
        fecha_reg = pd.to_datetime(row.get('timestamp', row.get('Date')), errors='coerce')

        hechos.append({
            'id_empleado': id_empleado,
            'id_trabajo': id_trabajo,
            'id_demografia': id_demografia,
            'id_tiempo': id_tiempo,
            'id_soporte': id_soporte,
            'nivel_estres': nivel_estres,
            'balance_work_life': balance,
            'productividad': productividad,
            'indice_bienestar': indice_bienestar,
            'accede_soporte': accede_soporte,
            'satisfaccion_laboral': _mapear_productividad(
                row.get('Mental_Health_Condition') or row.get('treatment', 3)
            ),
            'fecha_registro': fecha_reg,
        })

    df = pd.DataFrame(hechos)
    logger.info(f"fact_bienestar_laboral creada: {len(df)} registros")
    return df


def _buscar_clave_dim(dim: pd.DataFrame, row: pd.Series, columnas: list, mapeo_campos: dict = None) -> int:
    """
    Busca la clave primaria de una dimensión que coincida con los datos de la fila.
    Usa match parcial: mientras más columnas coincidan, mejor.

    Args:
        dim: DataFrame de la dimensión.
        row: Fila de datos crudos.
        columnas: Lista de nombres de columnas de la dimensión a comparar.
        mapeo_campos: Diccionario que mapea columnas de dimensión a columnas del dataset.

    Returns:
        ID de la dimensión encontrada, o 1 si no hay coincidencia.
    """
    if mapeo_campos is None:
        mapeo_campos = {}

    # Determinar el prefijo del ID basado en el nombre de la dimensión
    id_col = [c for c in dim.columns if c.startswith('id_')][0]

    # Por cada fila de la dimensión, contar cuántas columnas coinciden
    mejor_puntaje = -1
    mejor_id = dim[id_col].iloc[0]

    for idx, dim_row in dim.iterrows():
        puntaje = 0
        for col in columnas:
            # Obtener el valor del dataset
            campo_dataset = mapeo_campos.get(col, col)
            valor = row.get(campo_dataset)

            # Si el valor existe y coincide con la dimensión
            if valor is not None and col in dim.columns:
                if dim[col].dtype == 'object':
                    if dim_row[col].lower() == str(valor).lower():
                        puntaje += 1
                else:
                    if dim_row[col] == valor:
                        puntaje += 1

        # Si este puntaje es mejor que el anterior, guardar
        if puntaje > mejor_puntaje:
            mejor_puntaje = puntaje
            mejor_id = dim_row[id_col]

    return mejor_id


def _determinar_soporte(row: pd.Series, dim_soporte: pd.DataFrame) -> int:
    """
    Determina qué tipo de soporte tiene basado en los datos de la fila.

    Args:
        row: Fila de datos crudos.
        dim_soporte: Dimensión de soporte.

    Returns:
        ID del tipo de soporte.
    """
    acceso = _verificar_soporte(row)

    if acceso:
        return dim_soporte[
            (dim_soporte['tipo_soporte'] == 'Employee Assistance Program') &
            (dim_soporte['disponible'] == True)
        ]['id_soporte'].iloc[0]
    else:
        return dim_soporte[
            dim_soporte['tipo_soporte'] == 'No Support Available'
        ]['id_soporte'].iloc[0]


def _verificar_soporte(row: pd.Series) -> bool:
    """
    Verifica si la fila indica acceso a soporte de salud mental.

    Args:
        row: Fila de datos crudos.

    Returns:
        True si tiene acceso, False en caso contrario.
    """
    campos_soporte = [
        'Access_to_Mental_Health_Resources',
        'benefits',
        'care_options',
        'wellness_program',
        'seek_help',
    ]

    for campo in campos_soporte:
        if campo in row.index:
            valor = str(row[campo]).lower()
            if valor in ['yes', 'true', '1', 'available', 'offered']:
                return True

    return False
