# LATAM Ops: alerta temprana T-2 h de demoras de salida (ANAC VRA + METAR) con Spark en AWS.
# `make help` lista todas las metas. Las metas de infraestructura vienen de infra/infra.mk.

SHELL := /bin/bash
.DEFAULT_GOAL := help
-include infra/infra.mk

DESDE ?= 2023-01
HASTA ?= 2025-12
MES ?= 2026-01
ESCALON ?= E1
DC := docker compose -f docker/docker-compose.yml
REMOTO := scripts/ejecutar_remoto.sh
PASO_REMOTO = /opt/proyecto/scripts/ejecutar_paso.sh
SPARK_REMOTO = cd /opt/proyecto && LATAM_MODO=aws PYTHONPATH=src spark-submit

.PHONY: help imagen test lint pipeline-local demo-local jupyter-local datos-local ingesta silver gold modelo predecir \
        kpi catalogar pipeline demo bench-opt bench-escala bench-una-maquina deriva valor valor-local exportar-tablero

help: ## Lista las metas disponibles
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local (Docker, sin AWS)
imagen: ## Construye la imagen Docker de pruebas (Python 3.11 + JDK 17 + PySpark 3.5)
	@echo ">> docker build latam-ops-tests:1.0"
	$(DC) build tests

test: imagen ## Ejecuta las pruebas (pytest) dentro de Docker
	@echo ">> pytest en Docker"
	$(DC) run --rm tests pytest -q

lint: imagen ## Ruff sobre src/ y tests/
	@echo ">> ruff check"
	$(DC) run --rm tests ruff check src tests scripts

pipeline-local: imagen ## Pipeline completo en Docker sobre la muestra real (data/muestra)
	@echo ">> pipeline local sobre data/muestra"
	$(DC) run --rm pipeline

datos-local: imagen ## Pipeline local sobre data/raw ya descargado: DESDE=AAAA-MM HASTA=AAAA-MM
	@echo ">> pipeline local sobre data/raw $(DESDE)..$(HASTA)"
	$(DC) run --rm -e LATAM_LAKE_LOCAL=/app/data/lake tests python -m latam_ops.orquestacion.ejecutar_paso todo \
	  --desde $(DESDE) --hasta $(HASTA) --origen local

demo-local: imagen ## Demo de punta a punta en local con el mes MES (por defecto 2026-01)
	@echo ">> demo local con $(MES)"
	$(DC) run --rm -e LATAM_LAKE_LOCAL=/app/data/lake tests bash scripts/demo_subir_mes.sh $(MES) local

jupyter-local: imagen ## JupyterLab en http://127.0.0.1:8888 con los notebooks
	@echo ">> jupyter local"
	$(DC) up jupyter

# ---------------------------------------------------------------- en el clúster AWS (vía SSM)
ingesta: ## Ingesta VRA + METAR a bronze en el clúster: DESDE= HASTA=
	@echo ">> ingesta $(DESDE)..$(HASTA) en el master"
	$(REMOTO) "$(PASO_REMOTO) ingerir --desde $(DESDE) --hasta $(HASTA)"

silver: ## bronze -> silver en el clúster
	@echo ">> silver $(DESDE)..$(HASTA)"
	$(REMOTO) "$(PASO_REMOTO) silver --desde $(DESDE) --hasta $(HASTA)"

gold: ## silver -> gold/features_salida en el clúster
	@echo ">> gold $(DESDE)..$(HASTA)"
	$(REMOTO) "$(PASO_REMOTO) gold --desde $(DESDE) --hasta $(HASTA)"

modelo: ## Modelo base + modelo final (RF/GBT, umbral por costo) en el clúster
	@echo ">> modelo base y final"
	$(REMOTO) "$(PASO_REMOTO) modelo_base && $(PASO_REMOTO) modelo_final"

predecir: ## Predicciones de valid, test y demo
	@echo ">> predicciones"
	$(REMOTO) "$(PASO_REMOTO) predecir --desde 2024-07 --hasta $(MES)"

kpi: ## Estrella (fact + dimensiones) y KPI
	@echo ">> estrella y KPI"
	$(REMOTO) "$(PASO_REMOTO) kpi --desde $(DESDE) --hasta $(MES)"

deriva: ## Monitoreo de deriva (PSI)
	@echo ">> deriva"
	$(REMOTO) "$(PASO_REMOTO) deriva"

valor: ## Valor de negocio: línea base, ahorro por escenario, sensibilidad y trazabilidad
	@echo ">> valor de negocio"
	$(REMOTO) "$(PASO_REMOTO) valor"

valor-local: imagen ## Valor de negocio en local sobre el lake de data/lake
	@echo ">> valor de negocio en local"
	$(DC) run --rm -e LATAM_LAKE_LOCAL=/app/data/lake tests python -m latam_ops.ml.valor_negocio

catalogar: ## Lanza el crawler de Glue sobre gold
	@echo ">> crawler de gold"
	$(REMOTO) "$(PASO_REMOTO) catalogar"

pipeline: ## Pipeline completo en el clúster (ingesta -> silver -> gold -> modelos -> predicciones -> KPI -> catálogo)
	@echo ">> pipeline completo $(DESDE)..$(HASTA)"
	$(REMOTO) "$(PASO_REMOTO) todo --desde $(DESDE) --hasta $(HASTA) && $(PASO_REMOTO) catalogar"

demo: ## DEMO: sube el mes MES a bronze/landing y Step Functions corre todo hasta el tablero
	@echo ">> demo de punta a punta con $(MES)"
	scripts/demo_subir_mes.sh $(MES) aws

bench-opt: ## Optimizaciones O1-O4 con tiempos antes/después
	@echo ">> optimizaciones O1-O4"
	$(REMOTO) "$(SPARK_REMOTO) src/latam_ops/benchmark/optimizaciones.py --mes 2024-06"

bench-escala: ## Escalabilidad ESCALON=E1..E5 con el clúster actual (WORKERS=)
	@echo ">> escalabilidad $(ESCALON) con $(WORKERS) worker(s)"
	$(REMOTO) "$(SPARK_REMOTO) src/latam_ops/benchmark/escalabilidad.py --escalon $(ESCALON) --nodos $$(( $(WORKERS) + 1 ))"

bench-una-maquina: ## E6: el mismo cálculo en una sola máquina con pandas
	@echo ">> una sola máquina"
	$(REMOTO) "cd /opt/proyecto && LATAM_MODO=aws PYTHONPATH=src python3.11 -m latam_ops.benchmark.una_maquina --anio 2024"

exportar-tablero: ## Descarga gold a ./powerbi_data (Parquet + CSV para Power BI / Looker Studio)
	@echo ">> exportando tablas de consumo"
	scripts/exportar_tablero.sh
