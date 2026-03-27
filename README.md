# 🧠 Salud Mental en el Trabajo Remoto

*El Costo Oculto de Trabajar desde Casa — Pipeline Analytics End-to-End*

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)](https://docker.com)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)](https://powerbi.microsoft.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 🎯 ¿Por qué existe este proyecto?

El trabajo remoto pasó de ser excepción a norma para millones de trabajadores. Con eso emergió una crisis silenciosa: el deterioro de la salud mental laboral. Este proyecto construye un **pipeline analítico end-to-end** para cuantificar ese impacto, segmentado por industria, género y modalidad de trabajo.

## 🏗️ Arquitectura

```
CSV Kaggle → ETL Python → PostgreSQL (Docker) → Power BI Dashboard
```

## 📦 Stack tecnológico

| Capa | Herramientas |
|------|-------------|
| Ingesta | Python · pandas · Kaggle CLI |
| Transformación | pandas · numpy · loguru |
| Almacenamiento | PostgreSQL 15 · Docker · SQLAlchemy |
| Visualización | Power BI Desktop |
| Testing | pytest · pytest-cov |
| Versionado | Git · GitHub |

## 🚀 Inicio rápido

```bash
# 1. Clonar repositorio
git clone https://github.com/gtsuarez-analytics/mental-health-remote-work.git
cd mental-health-remote-work

# 2. Crear entorno virtual
python3 -m venv .venv && source .venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# 5. Descargar datasets (requiere cuenta Kaggle)
pip install kaggle
kaggle datasets download \
  waqi786/remote-work-and-mental-health \
  -p data/raw --unzip
kaggle datasets download \
  osmi/mental-health-in-tech-survey \
  -p data/raw --unzip

# Renombrar al nombre estándar del proyecto
mv data/raw/Impact_of_Remote_Work_on_Mental_Health.csv \
   data/raw/remote_work_mental_health.csv

# 6. Levantar PostgreSQL
docker compose up -d

# 7. Ejecutar pipeline ETL
python etl/pipeline.py

# 8. Ejecutar tests
pytest tests/ -v
```

## 📊 KPIs del Dashboard

| KPI | Meta | Fórmula | Estado |
|-----|------|---------|--------|
| Índice de bienestar laboral | ≥ 7/10 | `(6 - estrés + balance + prod) / 3 * 2` | — |
| Tasa de alto estrés | < 20% | `% empleados con estrés ≥ 4` | — |
| Cobertura de soporte mental | ≥ 80% | `% empleados con acceso a soporte` | — |
| Productividad sostenida | ≥ 60% | `% empleados con productividad ≥ 4` | — |

## 📁 Estructura del Proyecto

```
mental-health-remote-work/
├── data/
│   ├── raw/           # CSVs originales de Kaggle
│   ├── processed/     # CSVs transformados por ETL
│   └── exports/       # Exports adicionales
├── docs/               # Documentación técnica
│   └── 05_consultas_analiticas.md  # KPIs y consultas analíticas
├── database/
│   ├── migrations/    # Scripts DDL del star schema
│   └── seeds/         # Scripts SQL
│       ├── 02_views_kpis.sql          # Vistas de KPIs
│       └── 03_consultas_analiticas.sql # PA-01 a PA-06
├── etl/
│   ├── pipeline.py    # Orquestador principal
│   ├── extract/       # Extracción de datos
│   ├── transform/     # Transformaciones y KPIs
│   ├── load/          # Carga a PostgreSQL
│   └── utils/         # Logger y conector DB
├── tests/             # Suite de pruebas pytest
├── docker-compose.yml # Infraestructura PostgreSQL
├── requirements.txt   # Dependencias Python
└── README.md
```

## 🔧 Comandos útiles

| Acción | Comando |
|--------|---------|
| Activar entorno virtual | `source .venv/bin/activate` |
| Levantar Docker | `docker compose up -d` |
| Ver logs PostgreSQL | `docker compose logs -f postgres` |
| Ejecutar ETL | `python etl/pipeline.py` |
| Ejecutar tests | `pytest tests/ -v --cov=etl` |
| Conectar a DB | `docker exec -it mental_health_postgres psql -U admin -d mental_health_db` |
| Ver KPIs globales | `docker exec -i mental_health_postgres psql -U admin -d mental_health_db -c "SELECT * FROM v_kpis_globales;"` |
| Ver KPIs por modalidad | `docker exec -i mental_health_postgres psql -U admin -d mental_health_db -c "SELECT * FROM v_kpis_por_modalidad;"` |

## 👤 Autor

**Gerardo Suárez T.** — Data Engineer & Analytics

- GitHub: [@gtsuarez-analytics](https://github.com/gtsuarez-analytics)
- LinkedIn: [gerardo-suarez-](https://www.linkedin.com/in/gerardo-suarez-/)

---

*Proyecto de portafolio — Todos los derechos reservados © 2026 Gerardo Suárez T.*
