# Distributed Vector Search

Prototipo académico de búsqueda vectorial para Sistemas Distribuidos. En la Fase 1, el gateway
FastAPI enruta operaciones a réplicas de shards ejecutadas como actores Ray. El repositorio también
incluye `tools/stub_server`, un servidor en memoria que permite desarrollar el cliente y probar el
contrato HTTP sin desplegar Ray.

La Fase 1 sirve para observar qué aporta el runtime Ray antes de diseñar una implementación sin
Ray. No ofrece persistencia en disco ni failover durable. El stub no distribuye datos, no tiene
shards y no simula fallos ni respuestas parciales.

## Arquitectura de la Fase 1

- **SDK y CLI (`src/dvs/client`)**: cliente síncrono y asíncrono, generación de `request_id`,
  reintentos transitorios con backoff y jitter, tokens RYW e historiales JSONL.
- **Gateway (`src/dvs/api`, `src/dvs/coordinator`)**: API HTTP, validación y coordinación de
  operaciones sobre shards.
- **Runtime (`src/dvs/ray_impl`)**: catálogo y réplicas de shard como actores Ray. La configuración
  incluida define 4 shards, factor de replicación 3 y quorum de escritura 2; el estado vive en
  memoria.
- **Despliegue (`deploy/ray`)**: un head, tres workers y un gateway HTTP mediante Docker Compose.
- **Stub (`tools/stub_server`)**: backend independiente en memoria para pruebas del contrato y del
  cliente.
- **Experimentos (`experiments`)**: generación de datasets y ground truth, harness E1/E2/E3,
  análisis de latencia y recall, checker de E3 e inyector de fallos para el Compose del proyecto.

## Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `src/dvs/core/` | Núcleo de shard y modelos compartidos |
| `src/dvs/api/`, `src/dvs/coordinator/` | API HTTP y coordinación |
| `src/dvs/ray_impl/` | Actores y transporte de la Fase 1 |
| `src/dvs/client/` | SDK Python y CLI `dvs` |
| `tools/stub_server/` | Servidor simulado T119b |
| `experiments/` | Datos, workloads, análisis y fault injection |
| `deploy/ray/` | Dockerfile, Compose y configuraciones Ray |
| `tests/unit/`, `tests/contract/`, `tests/integration/` | Pruebas unitarias, HTTP y de integración |
| `docs/`, `.sdd/` | Documentación de tareas, Fase 1 y especificaciones locales |
| `runs/` | Evidencia de experimentos; los resultados locales no se añaden a Git |

## Requisitos e instalación

Se requiere Python 3.12. Para el despliegue Ray se requiere Docker Engine o Docker Desktop con
Docker Compose v2. GNU Make permite usar los atajos del `Makefile`; desde Windows PowerShell se
pueden usar los comandos Python y Docker mostrados abajo directamente.

### Windows PowerShell

Desde la raíz del repositorio:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,ray]"
```

Si la política de PowerShell no permite activar el entorno, ejecuta el intérprete explícitamente:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,ray]"
```

En Linux, macOS o WSL, el atajo equivalente es `make install` (crea `.venv` e instala los extras
`dev` y `ray`).

## Configuración y uso del cliente

El Compose y el stub usan claves de desarrollo definidas para pruebas locales. No las uses fuera
del proyecto. `demo` tiene dimensión 128 y métrica L2; `demo_cos` tiene dimensión 128 y métrica
coseno.

Para levantar el stub en una terminal:

```powershell
python -m tools.stub_server
```

En otra terminal PowerShell configura el cliente y ejecuta operaciones:

```powershell
$env:DVS_BASE_URL = "http://127.0.0.1:8080"
$env:DVS_API_KEY = "dev-key-rw"
python -c "import json; open('vector.json', 'w', encoding='utf-8').write(json.dumps([1.0] + [0.0] * 127))"
dvs health
dvs upsert demo item-1 --vector "@vector.json" --metadata '{"source":"demo"}'
dvs search demo --vector "@vector.json" --k 10
```

Para cargar el dataset generado en la colección:

```powershell
python -m experiments.data_gen --name demo --metric L2 --seed 20261125 --n 5000 --dim 128 --n-clusters 32 --n-queries 200 --k-gt 10 --output-dir data
dvs load demo --data-dir data/demo --batch-size 500
```

Los vectores deben coincidir con la dimensión configurada para la colección. El SDK también
ofrece `AsyncDVSClient`; ambos clientes aceptan varios gateways en `DVS_BASE_URL`, tokens RYW e
historial JSONL mediante `history_path` o `DVS_HISTORY_PATH`.

## Despliegue Ray con Docker Compose

Desde la raíz del repositorio, con el daemon de Docker disponible:

```powershell
$env:DVS_RUN_ID = "f1-local"
docker compose -f deploy/ray/docker-compose.yml up --build --detach --wait --wait-timeout 300
docker compose -f deploy/ray/docker-compose.yml exec -T ray-head ray status --address=127.0.0.1:6379
docker compose -f deploy/ray/docker-compose.yml exec -T ray-head python /workspace/deploy/ray/check_cluster.py --expected-nodes 4
```

El gateway queda disponible en `http://127.0.0.1:8080`; el dashboard Ray se publica en
`http://127.0.0.1:8265`. El comando de comprobación valida cuatro nodos Ray (head y tres workers).
Para apagar los servicios:

```powershell
docker compose -f deploy/ray/docker-compose.yml down --remove-orphans
```

Los atajos `make up-ray` y `make down-ray` ejecutan el mismo ciclo desde un entorno con GNU Make.
La configuración de despliegue está en [`deploy/ray/README.md`](deploy/ray/README.md). Si el puerto
8080 está ocupado, establece `DVS_GATEWAY_PORT` antes del `up`.

## Pruebas y verificaciones

Desde un entorno con el paquete instalado:

```powershell
python -m ruff check .
python -m mypy
python -m pytest
```

`python -m mypy` comprueba los directorios configurados en `pyproject.toml` (`src/dvs/core` y
`src/dvs/coordinator`). `make check` reúne Ruff, mypy y las pruebas unitarias. Las pruebas de
contrato requieren un servidor activo y `DVS_BASE_URL`:

```powershell
$env:DVS_BASE_URL = "http://127.0.0.1:8080"
$env:DVS_API_KEY = "dev-key-rw"
python -m pytest tests/contract
```

Consulta [`tests/contract/README.md`](tests/contract/README.md) y
[`tools/stub_server/README.md`](tools/stub_server/README.md) para sus alcances. Las pruebas de
integración Ray son separadas y requieren el runtime Ray local; no se ejecutan con la suite
unitaria ordinaria. El target definido para ellas es `make test-integration` (o
`python -m pytest tests/integration -m integration`).

## Datos y experimentos

El generador `experiments.data_gen` usa una semilla explícita y escribe vectores, metadatos,
consultas y ground truth exacto. `make data` genera los datasets L2 y cosine configurados en el
`Makefile`. Cada ejecución del harness genera su propio dataset bajo `runs/<run_id>/dataset/` y
rechaza reutilizar un directorio que ya contiene resultados.

Con el stub activo, ejecuta un baseline E2:

```powershell
python -m experiments.runner --scenario e2 --target-kind stub --base-url http://127.0.0.1:8080 --run-id e2-stub-01
```

En Linux/macOS/WSL, el atajo del `Makefile` para el mismo escenario es:

```sh
make exp SCENARIO=e2 EXP_ARGS="--target-kind stub --base-url http://127.0.0.1:8080 --run-id e2-stub-01"
```

Para ejecutar E3 en el stub:

```powershell
python -m experiments.runner --scenario e3 --target-kind stub --base-url http://127.0.0.1:8080 --run-id e3-stub-01 --queries 20
```

E3 ejecuta lectores y escritores concurrentes sobre IDs compartidos y verifica que `metadata.ver`
coincida con `vector[0]`. Un baseline en el stub valida el harness y el contrato del stub; no
demuestra propiedades de actores Ray, distribución ni resiliencia.

El harness escribe `run.json`, `client/latencies.csv`, historiales JSONL, snapshots de estado y
métricas, dataset/ground truth y `report.md`. Calcula p50/p95/p99 por operación con interpolación
lineal de NumPy, incluyendo errores; la tasa de error usa todas las operaciones, el porcentaje de
parciales solo búsquedas y recall@k es `|resultados ∩ ground_truth| / min(k, |ground_truth|)`.
Una búsqueda fallida aporta recall cero. Las ejecuciones se guardan bajo `runs/`, directorio
ignorado por Git.

## Inyección de fallos (T120b)

Los fallos Docker/SO solo apuntan a contenedores etiquetados del proyecto Compose `dvs-f1`. La
herramienta verifica esas etiquetas antes de actuar y registra solicitud, aplicación y recuperación
en `faults.jsonl`, con UTC y tiempo monotónico. La recuperación comprueba que Ray vuelva a mostrar
cuatro nodos y que el endpoint de salud responda. Usa el inyector solo con el Compose de este
repositorio.

Inspecciona servicios y réplicas antes de elegir un objetivo:

```powershell
python -m experiments.faults --run-dir runs/fault-inspection info
```

Escenarios disponibles: `docker-kill` (detiene un servicio Compose y lo vuelve a levantar),
`kill-pid` (termina el PID de un actor Ray seleccionado), `docker-pause`, `netem` y
`disconnect`. Por ejemplo, una pausa controlada:

```powershell
python -m experiments.faults --run-dir runs/fault-pause run --kind docker-pause --target ray-worker-1 --duration-seconds 5
```

`netem` está limitado a los workers y necesita `NET_ADMIN`, concedido por el Compose. Solo añade
una regla cuando la interfaz parte del qdisc por defecto y elimina únicamente la regla que creó.
El modo `ray-delay` llama al gancho T120a ya implementado; es un retraso dentro del actor, no una
degradación real de la red. No uses el stub para afirmar resultados de fallo Ray.

Para una carga con retraso de shard en Ray, elige un `replica_id` mostrado por `info` y úsalo en
el harness:

```powershell
python -m experiments.runner --scenario e2 --target-kind ray --base-url http://127.0.0.1:8080 --run-id e2-ray-delay-01 --fault-kind ray-delay --fault-target <replica_id> --fault-ms 200
```

E1 también requiere un clúster Ray activo y un `replica_id` válido. El harness lo combina con carga
mixta y `kill-pid`:

```powershell
python -m experiments.runner --scenario e1 --target-kind ray --base-url http://127.0.0.1:8080 --run-id e1-ray-kill-01 --fault-kind kill-pid --fault-target <replica_id>
```

Sustituye `<replica_id>` por uno que haya devuelto `info`; consulta también
`python -m experiments.runner --help` antes de elegir parámetros. Las ejecuciones de sistema y
resiliencia son distintas de los tests unitarios y no deben describirse como realizadas si no se
ejecutaron en el clúster real.

## Documentación del proyecto

- [Plan de trabajo](docs/PLAN_DE_TRABAJO.md) y [reparto de tareas](docs/REPARTO_TAREAS.md).
- [Contrato HTTP](docs/fase1_ray/01_contrato_api.md), [arquitectura Ray](docs/fase1_ray/02_arquitectura_ray.md)
  y [registro del experimento inicial](docs/fase1_ray/04_experimento_inicial.md).
- [Despliegue Ray](deploy/ray/README.md), el [SDK y CLI](src/dvs/client/) y
  [stub de T119b](tools/stub_server/README.md).
- [Declaración de uso de IA](ai_use.yaml).

## Uso de IA, terceros y licencias

La metodología local y los documentos SDD no definen una plantilla ni campos obligatorios para
`ai_use.yaml`; este repositorio adopta un formato YAML mínimo que separa herramienta, modelo,
actividades y alcance verificable. El archivo declara el uso de Codex/OpenAI GPT-6 en esta
contribución. No representa el historial completo del equipo ni atribuye uso a otras personas.

Ray es el runtime de terceros usado en Fase 1. NumPy, Pydantic, PyYAML, FastAPI, Uvicorn y HTTPX
son dependencias directas declaradas en `pyproject.toml`. Para esos paquetes, la tabla muestra
las versiones presentes en `.venv` durante esta validación y las licencias de sus metadatos; no
fija las versiones permitidas por el proyecto. Ray no está instalado en el entorno anfitrión: el
proyecto fija `2.60.1`, cuya ficha de [PyPI declara Apache 2.0](https://pypi.org/project/ray/2.60.1/).

| Paquete | Versión observada | Licencia declarada |
|---|---:|---|
| [Ray](https://pypi.org/project/ray/2.60.1/) | 2.60.1 | Apache 2.0 |
| [NumPy](https://pypi.org/project/numpy/2.5.5/) | 2.5.5 | BSD-3-Clause, 0BSD, MIT, Zlib y CC0-1.0 |
| [Pydantic](https://pypi.org/project/pydantic/2.14.4/) | 2.14.4 | MIT |
| [PyYAML](https://pypi.org/project/pyyaml/6.0.4/) | 6.0.4 | MIT |
| [FastAPI](https://pypi.org/project/fastapi/0.165.0/) | 0.165.0 | MIT |
| [Uvicorn](https://pypi.org/project/uvicorn/0.54.0/) | 0.54.0 | BSD-3-Clause |
| [HTTPX](https://pypi.org/project/httpx/0.29.0/) | 0.29.0 | BSD-3-Clause |
| [pytest](https://pypi.org/project/pytest/9.2.0/) (desarrollo) | 9.2.0 | MIT |
| [pytest-asyncio](https://pypi.org/project/pytest-asyncio/1.5.0/) (desarrollo) | 1.5.0 | Apache-2.0 |
| [Hypothesis](https://pypi.org/project/hypothesis/6.170.17/) (desarrollo) | 6.170.17 | MPL-2.0 |
| [Ruff](https://pypi.org/project/ruff/0.17.6/) (desarrollo) | 0.17.6 | MIT |
| [mypy](https://pypi.org/project/mypy/2.4.1/) (desarrollo) | 2.4.1 | MIT |

La declaración del entorno no enumera exhaustivamente las dependencias transitivas ni reemplaza
los avisos de licencia distribuidos con cada paquete. Faiss, Milvus y Weaviate aparecen en la
documentación como referencias comparativas; no
están declarados como dependencias del proyecto. Los nombres y marcas pertenecen a sus titulares.
El proyecto es independiente y no está afiliado, patrocinado ni respaldado por esos titulares.
No se encontró un archivo `LICENSE` en la raíz del repositorio; este aviso tampoco concede una
licencia al código del proyecto.

## Limitaciones conocidas

- La Fase 1 conserva el estado en memoria; no implementa persistencia durable ni failover.
- El stub es un proceso único y no permite probar distribución, pérdida de shards ni respuestas
  parciales.
- Las propiedades de recuperación y degradación de red requieren ejecutar los escenarios contra
  el Compose Ray real.
- Los checks de metadatos de dependencias reflejan el entorno de validación. Las versiones no
  fijadas pueden resolver a versiones diferentes al instalar en otra fecha.
