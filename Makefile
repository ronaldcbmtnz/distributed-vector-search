# Makefile del proyecto. Rutas relativas a propósito: la carpeta del repo puede contener espacios.
# Cada sección tiene dueño (ver docs/REPARTO_TAREAS.md §2). Un target pendiente falla avisando su tarea.

VENV   ?= .venv
PY     := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip

.DEFAULT_GOAL := help
.PHONY: help venv install test lint format typecheck check clean \
        test-integration run-ray-local \
        data stub test-contract up-ray down-ray exp

help: ## Lista los targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-18s %s\n",$$1,$$2}'

# ───────────────────────── común ─────────────────────────
venv: ## Crea el entorno virtual (.venv) con Python 3.12
	python3.12 -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv ## Instala el paquete en modo editable con extras dev y ray
	$(PIP) install -e ".[dev,ray]"

test: ## Tests unitarios (rápidos, sin Ray ni red)
	$(PY) -m pytest

lint: ## Lint con ruff
	$(PY) -m ruff check .

format: ## Formatea con ruff
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .

typecheck: ## mypy estricto sobre core/ y coordinator/
	$(PY) -m mypy

check: lint typecheck test ## Todo lo anterior (antes de cada PR)

clean: ## Borra cachés
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

# ───────────────────────── A · sistema ─────────────────────────
test-integration: ## [A] Tests de integración con Ray local
	$(PY) -m pytest tests/integration -m integration

run-ray-local: ## [A] Gateway + Ray local en :8080 (T115)
	@echo "pendiente: T115 [A]"; exit 1

# ───────────────────────── B · plataforma y verificación ─────────────────────────
data: ## [B] Genera el dataset sintético con semilla (T010)
	@echo "pendiente: T010 [B]"; exit 1

stub: ## [B] Levanta el servidor simulado del contrato en :8080 (T119b)
	@echo "pendiente: T119b [B]"; exit 1

test-contract: ## [B] Tests de contrato contra DVS_BASE_URL (T119)
	$(PY) -m pytest tests/contract

up-ray: ## [B] Levanta el clúster Ray con Docker Compose (T118)
	@echo "pendiente: T118 [B]"; exit 1

down-ray: ## [B] Para el clúster Ray (T118)
	@echo "pendiente: T118 [B]"; exit 1

exp: ## [B] Ejecuta un escenario: make exp SCENARIO=e2 (T121a)
	@echo "pendiente: T121a [B]"; exit 1
