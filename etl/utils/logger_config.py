"""
================================================================================
CONFIGURACIÓN DE LOGGING — SALUD MENTAL EN EL TRABAJO REMOTO
================================================================================

Módulo centralizado de configuración de logging con loguru.
Proporciona logs en consola con colores y en archivo rotativo.

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import sys
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()


def configurar_logger(nombre_modulo: str) -> None:
    """
    Configura el logger de loguru para un módulo específico.

    Args:
        nombre_modulo: Nombre del módulo que usa el logger.
    """
    # Remover handlers previos
    logger.remove()

    # Configurar formato
    formato = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        f"<cyan>{nombre_modulo}</cyan>:<cyan>{{name}}</cyan>:<cyan>{{function}}</cyan> | "
        "<level>{message}</level>"
    )

    # Handler para consola
    logger.add(
        sys.stdout,
        format=formato,
        level=os.getenv("LOG_LEVEL", "INFO"),
        colorize=True,
    )

    # Handler para archivo rotativo
    log_path = Path(os.getenv("LOG_PATH", "logs"))
    log_path.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_path / "mental_health_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        format=formato,
        level=os.getenv("LOG_LEVEL", "INFO"),
        colorize=False,
    )

    logger.info(f"Logger configurado para {nombre_modulo}")
