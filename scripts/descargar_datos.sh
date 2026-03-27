#!/bin/bash
# ================================================================================
# DESCARGA DE DATASETS — SALUD MENTAL EN EL TRABAJO REMOTO
# ================================================================================
# Script para descargar los datasets de Kaggle.
# Requiere: kaggle CLI instalado y configurado con credenciales.
#
# Uso: bash scripts/descargar_datos.sh
# ================================================================================

set -e

echo "=============================================="
echo "Descargando datasets de Kaggle..."
echo "=============================================="

# Crear directorio si no existe
mkdir -p data/raw

# Dataset 1: Remote Work and Mental Health
echo ">>> Descargando: Remote Work and Mental Health"
kaggle datasets download \
  waqi786/remote-work-and-mental-health \
  -p data/raw \
  --unzip \
  -q

# Renombrar al nombre estándar
if [ -f "data/raw/Impact_of_Remote_Work_on_Mental_Health.csv" ]; then
  mv data/raw/Impact_of_Remote_Work_on_Mental_Health.csv \
     data/raw/remote_work_mental_health.csv
  echo "    ✓ Renombrado a remote_work_mental_health.csv"
fi

# Dataset 2: Mental Health in Tech Survey
echo ">>> Descargando: Mental Health in Tech Survey"
kaggle datasets download \
  osmi/mental-health-in-tech-survey \
  -p data/raw \
  --unzip \
  -q

# Renombrar si existe
if [ -f "data/raw/survey.csv" ]; then
  mv data/raw/survey.csv data/raw/mental_health_tech_survey.csv
  echo "    ✓ Renombrado a mental_health_tech_survey.csv"
fi

echo ""
echo "=============================================="
echo "Descarga completada"
echo "=============================================="
echo ""
echo "Archivos disponibles:"
ls -lh data/raw/*.csv
echo ""
echo "Siguiente paso: python etl/pipeline.py"
