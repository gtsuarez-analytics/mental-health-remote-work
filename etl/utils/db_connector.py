"""
================================================================================
CONECTOR DE BASE DE DATOS — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo para gestión de conexiones a PostgreSQL usando SQLAlchemy.
Proporciona un conector reutilizable con manejo de errores y logging.

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def obtener_engine() -> Engine:
    """
    Crea y retorna un engine de SQLAlchemy para PostgreSQL.

    Returns:
        Engine de SQLAlchemy conectado a la base de datos.

    Raises:
        SQLAlchemyError: Si no se puede conectar a la base de datos.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_name]):
        logger.error("Faltan variables de entorno requeridas para la conexión")
        raise ValueError(
            "DB_USER, DB_PASSWORD, DB_NAME son requeridos en el archivo .env"
        )

    connection_string = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    try:
        engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
        )
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Conexión exitosa a PostgreSQL: {db_host}:{db_port}/{db_name}")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Error al conectar a PostgreSQL: {e}")
        raise


def verificar_conexion(engine: Engine) -> bool:
    """
    Verifica que la conexión a la base de datos está activa.

    Args:
        engine: Engine de SQLAlchemy a verificar.

    Returns:
        True si la conexión está activa, False en caso contrario.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError as e:
        logger.error(f"La conexión a la base de datos falló: {e}")
        return False
