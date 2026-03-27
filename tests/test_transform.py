"""
================================================================================
PRUEBAS UNITARIAS — TRANSFORMACIÓN ETL
================================================================================

Suite de pruebas para las funciones críticas del módulo de transformación.
Cubre: normalización de géneros, cálculo del índice de bienestar,
construcción de dimensiones y tabla de hechos.

Autor: Gerardo Suárez T. (gtsuarez-analytics)
Proyecto: Salud Mental en el Trabajo Remoto
================================================================================
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.transform.build_dimensions import (
    _normalizar_genero,
    _clasificar_rango_etario,
    _clasificar_empresa_tamano,
    construir_dim_tiempo,
    construir_dim_trabajo,
    construir_dim_demografia,
    construir_dim_soporte,
)
from etl.transform.build_fact_table import (
    _calcular_indice_bienestar,
    _mapear_nivel_estres,
    _mapear_productividad,
    _anonimizar_id,
)


# ── Pruebas de normalización de género ───────────────────────────

class TestNormalizarGenero:
    """Pruebas para la función _normalizar_genero."""

    def test_male_variations(self):
        """Verifica que variantes de 'male' se normalicen correctamente."""
        assert _normalizar_genero("Male") == "Male"
        assert _normalizar_genero("male") == "Male"
        assert _normalizar_genero("M") == "Male"
        assert _normalizar_genero("Man") == "Male"
        assert _normalizar_genero("Masculino") == "Male"

    def test_female_variations(self):
        """Verifica que variantes de 'female' se normalicen correctamente."""
        assert _normalizar_genero("Female") == "Female"
        assert _normalizar_genero("female") == "Female"
        assert _normalizar_genero("F") == "Female"
        assert _normalizar_genero("Woman") == "Female"
        assert _normalizar_genero("Femenino") == "Female"

    def test_other_cases(self):
        """Verifica que valores no reconocidos sean 'Other'."""
        assert _normalizar_genero("Non-binary") == "Other"
        assert _normalizar_genero("") == "Other"
        assert _normalizar_genero("Prefer not to say") == "Other"

    def test_nan_handling(self):
        """Verifica que valores NaN sean 'Other'."""
        assert _normalizar_genero(None) == "Other"
        assert _normalizar_genero(pd.NA) == "Other"


# ── Pruebas de rango etario ───────────────────────────────────────

class TestClasificarRangoEtario:
    """Pruebas para la función _clasificar_rango_etario."""

    def test_rangos_correctos(self):
        """Verifica clasificación correcta de edades."""
        assert _clasificar_rango_etario(20) == "18-25"
        assert _clasificar_rango_etario(30) == "26-35"
        assert _clasificar_rango_etario(40) == "36-45"
        assert _clasificar_rango_etario(50) == "46-55"
        assert _clasificar_rango_etario(60) == "56-65"
        assert _clasificar_rango_etario(70) == "65+"

    def test_extremos(self):
        """Verifica处理 de edades en extremos."""
        assert _clasificar_rango_etario(17) == "Under 18"
        assert _clasificar_rango_etario(100) == "65+"

    def test_valores_invalidos(self):
        """Verifica处理 de valores inválidos."""
        assert _clasificar_rango_etario(None) == "Unknown"
        assert _clasificar_rango_etario("") == "Unknown"
        assert _clasificar_rango_etario("abc") == "Unknown"


# ── Pruebas del índice de bienestar ──────────────────────────────

class TestIndiceBienestar:
    """Pruebas para la función _calcular_indice_bienestar."""

    def test_bienestar_alto(self):
        """Verifica índice alto con bajo estrés y alto balance/productividad."""
        # Estrés bajo (1), balance alto (5), productividad alta (5)
        # (6 - 1 + 5 + 5) / 3 * 2 = 10.0
        assert _calcular_indice_bienestar(1, 5, 5) == 10.0

    def test_bienestar_bajo(self):
        """Verifica índice bajo con alto estrés y bajo balance/productividad."""
        # Estrés alto (5), balance bajo (1), productividad baja (1)
        # (6 - 5 + 1 + 1) / 3 * 2 = 1.33... -> redondeado a 1.33
        resultado = _calcular_indice_bienestar(5, 1, 1)
        assert resultado >= 1.0 and resultado <= 2.0

    def test_bienestar_medio(self):
        """Verifica índice medio con valores neutrales."""
        # Estrés medio (3), balance medio (3), productividad media (3)
        # (6 - 3 + 3 + 3) / 3 * 2 = 6.0
        assert _calcular_indice_bienestar(3, 3, 3) == 6.0

    def test_limites(self):
        """Verifica que el índice se mantenga en rango 1-10."""
        # Valores extremos: con estrés máximo (5) y mínimos de balance/productividad (1)
        # (6 - 5 + 1 + 1) / 3 * 2 = 2.0 → clamp a 1.0 (mínimo teórico posible)
        assert _calcular_indice_bienestar(5, 1, 1) == 2.0
        # Con estrés mínimo (1) y máximos de balance/productividad (5)
        # (6 - 1 + 5 + 5) / 3 * 2 = 10.0 (máximo teórico posible)
        assert _calcular_indice_bienestar(1, 5, 5) == 10.0


# ── Pruebas de mapeo de estrés ──────────────────────────────

class TestMapearNivelEstres:
    """Pruebas para la función _mapear_nivel_estres."""

    def test_valores_numericos(self):
        """Verifica mapeo de valores numéricos."""
        assert _mapear_nivel_estres(1) == 1
        assert _mapear_nivel_estres(3) == 3
        assert _mapear_nivel_estres(5) == 5

    def test_valores_texto(self):
        """Verifica mapeo de valores de texto."""
        assert _mapear_nivel_estres("Low") == 1
        assert _mapear_nivel_estres("High") == 5
        assert _mapear_nivel_estres("Medium") == 3

    def test_valores_default(self):
        """Verifica valor por defecto para valores desconocidos."""
        assert _mapear_nivel_estres("Unknown") == 3
        assert _mapear_nivel_estres(None) == 3


# ── Pruebas de mapeo de productividad ──────────────────────────────

class TestMapearProductividad:
    """Pruebas para la función _mapear_productividad."""

    def test_valores_numericos(self):
        """Verifica mapeo de valores numéricos."""
        assert _mapear_productividad(1) == 1
        assert _mapear_productividad(5) == 5

    def test_valores_texto(self):
        """Verifica mapeo de valores de texto."""
        assert _mapear_productividad("Very Low") == 1
        assert _mapear_productividad("Excellent") == 5
        assert _mapear_productividad("Average") == 3


# ── Pruebas de anonimización ──────────────────────────────

class TestAnonimizarID:
    """Pruebas para la función _anonimizar_id."""

    def test_diferentes_entrada_diferentes_salida(self):
        """Verifica que diferentes entradas produzcan diferentes hashes."""
        hash1 = _anonimizar_id("usuario1")
        hash2 = _anonimizar_id("usuario2")
        assert hash1 != hash2

    def test_misma_entrada_misma_salida(self):
        """Verifica que la misma entrada siempre produzca el mismo hash."""
        hash1 = _anonimizar_id("usuario1")
        hash2 = _anonimizar_id("usuario1")
        assert hash1 == hash2

    def test_longitud_hash(self):
        """Verifica que el hash tenga la longitud correcta (16 caracteres)."""
        hash_val = _anonimizar_id("test")
        assert len(hash_val) == 16


# ── Pruebas de dimensiones ───────────────────────────────────────

class TestDimensiones:
    """Pruebas para funciones de construcción de dimensiones."""

    def test_dim_tiempo_creacion(self):
        """Verifica que dim_tiempo se cree correctamente."""
        df = construir_dim_tiempo(anno=2024)
        assert len(df) == 12  # 12 meses
        assert "id_tiempo" in df.columns
        assert "anno" in df.columns
        assert "mes" in df.columns
        assert "trimestre" in df.columns
        assert "semestre" in df.columns

    def test_dim_tiempo_trimestres(self):
        """Verifica cálculo correcto de trimestres."""
        df = construir_dim_tiempo(anno=2024)
        # Mes 1-3 -> trimestre 1, Mes 4-6 -> trimestre 2, etc.
        assert df[df["mes"] == 1]["trimestre"].iloc[0] == 1
        assert df[df["mes"] == 4]["trimestre"].iloc[0] == 2
        assert df[df["mes"] == 7]["trimestre"].iloc[0] == 3
        assert df[df["mes"] == 10]["trimestre"].iloc[0] == 4

    def test_dim_tiempo_semestres(self):
        """Verifica cálculo correcto de semestres."""
        df = construir_dim_tiempo(anno=2024)
        # Mes 1-6 -> semestre 1, Mes 7-12 -> semestre 2
        assert df[df["mes"] == 1]["semestre"].iloc[0] == 1
        assert df[df["mes"] == 6]["semestre"].iloc[0] == 1
        assert df[df["mes"] == 7]["semestre"].iloc[0] == 2
        assert df[df["mes"] == 12]["semestre"].iloc[0] == 2

    def test_dim_soporte_creacion(self):
        """Verifica que dim_soporte se cree correctamente."""
        df = construir_dim_soporte()
        assert len(df) == 5
        assert "id_soporte" in df.columns
        assert "tipo_soporte" in df.columns
        assert "disponible" in df.columns

    def test_dim_trabajo_creacion(self):
        """Verifica que dim_trabajo se cree con datos mínimos."""
        df_raw = pd.DataFrame({
            "Work_Location": ["Remote", "Hybrid", "In-Person"],
            "Industry": ["Tech", "Health", "Finance"],
            "Employment_Status": ["Full-time", "Part-time", "Contract"],
            "Company_Size": ["100-500", "26-100", "1000+"],
        })
        df = construir_dim_trabajo(df_raw)
        assert len(df) == 3
        assert "id_trabajo" in df.columns
        assert "modalidad" in df.columns

    def test_dim_demografia_creacion(self):
        """Verifica que dim_demografia se cree con datos mínimos."""
        df_raw = pd.DataFrame({
            "Gender": ["Male", "Female", "Other"],
            "Age": [25, 35, 45],
            "Country": ["USA", "Canada", "UK"],
        })
        df = construir_dim_demografia(df_raw)
        assert len(df) == 3
        assert "id_demografia" in df.columns
        assert "genero" in df.columns
        assert "rango_etario" in df.columns
