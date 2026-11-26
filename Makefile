# Makefile del proyecto. Rutas relativas a propósito: la carpeta del repo puede contener espacios.
# Cada sección tiene dueño (ver docs/REPARTO_TAREAS.md §2). Un target pendiente falla avisando su tarea.

VENV   ?= .venv
PY     := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
DATA_DIR ?= data
DATA_SEED ?= 20261125
DATA_N ?= 5000
DATA_DIM ?= 128
DATA_CLUSTERS ?= 32
DATA_QUERIES ?= 200
DATA_K ?= 10
COMPOSE ?= docker compose -f deploy/ray/docker-compose.yml

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

run-ray-local: ## [A] Gateway + Ray local (en proceso) en :8080; logs en runs/local
	DVS_CONFIG=deploy/ray/config.local.yaml DVS_RAY_ADDRESS=local DVS_RUN_DIR=runs/local \
		$(PY) -m uvicorn dvs.api.main_ray:app --host 127.0.0.1 --port 8080

# ───────────────────────── B · plataforma y verificación ─────────────────────────
data: ## [B] Genera el dataset sintético con semilla (T010)
	"$(PY)" -m experiments.data_gen --name demo --metric L2 --seed $(DATA_SEED) --n $(DATA_N) --dim $(DATA_DIM) --n-clusters $(DATA_CLUSTERS) --n-queries $(DATA_QUERIES) --k-gt $(DATA_K) --output-dir "$(DATA_DIR)"
	"$(PY)" -m experiments.data_gen --name demo_cos --metric cosine --seed $(DATA_SEED) --n $(DATA_N) --dim $(DATA_DIM) --n-clusters $(DATA_CLUSTERS) --n-queries $(DATA_QUERIES) --k-gt $(DATA_K) --output-dir "$(DATA_DIR)"

stub: ## [B] Levanta el servidor simulado del contrato en :8080 (T119b)
	$(PY) -m tools.stub_server

test-contract: ## [B] Tests de contrato contra DVS_BASE_URL (T119)
	$(PY) -m pytest tests/contract

up-ray: ## [B] Levanta el clúster Ray con Docker Compose (T118)
	$(COMPOSE) up --build --detach --wait --wait-timeout 300
	$(COMPOSE) exec -T ray-head ray status --address=127.0.0.1:6379
	$(COMPOSE) exec -T ray-head python /workspace/deploy/ray/check_cluster.py --expected-nodes 4

down-ray: ## [B] Para el clúster Ray (T118)
	$(COMPOSE) down --remove-orphans

exp: ## [B] Ejecuta un escenario: make exp SCENARIO=e2 (T121a)
	"$(PY)" -m experiments.runner --scenario $(or $(SCENARIO),e2) $(EXP_ARGS)
