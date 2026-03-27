# ================================================================================
# MAKEFILE — SALUD MENTAL EN EL TRABAJO REMOTO
# ================================================================================
# Automatización de comandos frecuentes del proyecto.
#
# Uso: make <comando>
# ================================================================================

.PHONY: help install setup test test-cov run-etl docker-up docker-down lint clean

# ── Variables ────────────────────────────────────────────────────────────────
VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
DOCKER_COMPOSE := docker compose

# ── Help ─────────────────────────────────────────────────────────────────────
help: ## Muestra esta ayuda
	@echo "=========================================="
	@echo "SALUD MENTAL TRABAJO REMOTO — Comandos"
	@echo "=========================================="
	@grep -E '^## |^##$$' $(MAKEFILE_LIST) | grep -v 'grep' | sed -e 's/## /make /' -e 's/##//'

# ── Entorno ──────────────────────────────────────────────────────────────────
install: ## Instala dependencias del proyecto
	pip install -r requirements.txt

setup: ## Configura el proyecto completo (entorno + Docker)
	@echo ">>> Creando entorno virtual..."
	python3 -m venv $(VENV_DIR)
	@echo ">>> Instalando dependencias..."
	$(PYTHON) -m pip install --upgrade pip
	pip install -r requirements.txt
	@echo ">>> Copiando archivo de entorno..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo ">>> Creando directorios..."
	mkdir -p data/processed data/exports logs

docker-up: ## Levanta los contenedores Docker
	$(DOCKER_COMPOSE) up -d
	@echo "Esperando a PostgreSQL..."
	@sleep 5
	@docker compose ps

docker-down: ## Detiene los contenedores Docker
	$(DOCKER_COMPOSE) down

docker-logs: ## Muestra logs de PostgreSQL
	$(DOCKER_COMPOSE) logs -f postgres

# ── ETL ──────────────────────────────────────────────────────────────────────
run-etl: ## Ejecuta el pipeline ETL completo
	$(PYTHON) etl/pipeline.py

run-extract: ## Solo fase de extracción
	$(PYTHON) -c "from etl.extract.extract_datasets import ejecutar_extraccion; ejecutar_extraccion()"

# ── Tests ─────────────────────────────────────────────────────────────────────
test: ## Ejecuta tests unitarios
	pytest tests/ -v

test-cov: ## Ejecuta tests con cobertura
	pytest tests/ -v --cov=etl --cov-report=term-missing --cov-report=html

test-unit: ## Solo tests de transformación
	pytest tests/test_transform.py -v

# ── Linting ───────────────────────────────────────────────────────────────────
lint: ## Ejecuta ruff linter
	ruff check etl/ tests/

lint-fix: ## Corrige errores de linting automáticamente
	ruff check etl/ tests/ --fix

format: ## Formatea código con ruff
	ruff format etl/ tests/

# ── Base de datos ─────────────────────────────────────────────────────────────
db-connect: ## Conecta a PostgreSQL via psql
	docker exec -it mental_health_postgres psql -U admin -d mental_health_db

db-reset: ## Resetea la base de datos (borra todo)
	@echo ">>> Reseteando base de datos..."
	$(DOCKER_COMPOSE) down -v
	$(DOCKER_COMPOSE) up -d
	@sleep 5

db-schema: ## Ejecuta script DDL del star schema
	docker exec -i mental_health_postgres psql -U admin -d mental_health_db < database/migrations/001_create_star_schema.sql

# ── Datos ─────────────────────────────────────────────────────────────────────
download-data: ## Descarga datasets de Kaggle
	bash scripts/descargar_datos.sh

clean-data: ## Limpia datos crudos y procesados
	rm -f data/raw/*.csv
	rm -f data/processed/*.csv

# ── Limpieza ──────────────────────────────────────────────────────────────────
clean: ## Limpia archivos temporales y caché
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	rm -rf htmlcov/
	rm -rf .coverage

clean-all: clean ## Limpia todo incluyendo entorno virtual
	rm -rf $(VENV_DIR)
