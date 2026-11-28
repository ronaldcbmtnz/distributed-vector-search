# Reparto de tareas y acuerdos de interfaz

> **Idea:** el proyecto se parte por el **contrato de la API** (`fase1_ray/01_contrato_api.md`),
> igual que un backend y un frontend se parten por su API.
>
> - **A — Sistema** (Ronald): todo lo que hay **detrás** del contrato. Núcleo de shard,
>   replicación, coordinador de búsquedas, Ray en F1, Raft y nodos nativos en F2, API REST.
> - **B — Plataforma y verificación** (compañero: _nombre pendiente_): todo lo que hay
>   **delante y alrededor**. Cliente/SDK, datos, tests de contrato, despliegue con Docker,
>   inyección de fallos, harness y análisis de experimentos.
>
> Cada uno trabaja en **carpetas distintas**. Los únicos puntos de contacto son los acuerdos de
> la §3: si ambos los respetan, ninguno necesita el código del otro hasta los puntos de unión
> (§4). B tiene aproximadamente el 30 % del trabajo pendiente.
>
> **Ojo:** los dos defienden el proyecto **completo**. Por eso cada uno revisa los PR del otro
> (§5), y B lee `00_introduccion.md` y el contrato antes de empezar.

---

## 1. Tareas de B (ver IDs en `PLAN_DE_TRABAJO.md`, etiquetadas `[B]`)

| ID | Tarea | Depende de | Hecho cuando |
|---|---|---|---|
| T005b | Instalar Docker Engine + Compose en su máquina | — | `docker compose version` responde sin `sudo` |
| T009 | `ai_use.yaml` + aviso de terceros en el `README.md` | — | Ficheros en el repo con el formato de la metodología |
| T010 | Generador de datos sintéticos con semilla + consultas + ground truth | §3.5 | `make data` reproduce ficheros idénticos |
| T116 | SDK/CLI del cliente: `request_id`, reintentos con backoff y jitter, lista de gateways, tokens, historias | §3.1, §3.6 | Tests del SDK verdes contra el stub (T119b) |
| T119b | **Servidor simulado del contrato** (en memoria, un proceso, sin distribución) | §3.1 | Los tests de contrato sin fallos pasan contra él |
| T119 | Tests de contrato (pytest + httpx) contra `DVS_BASE_URL` | §3.1, §3.2 | Verdes contra el stub; luego contra el sistema real (unión J1) |
| T118 | Docker Compose del clúster Ray: head + 3 workers + gateway, volumen `runs/`, `make up-ray` | §3.3 | El clúster se forma (`ray status` muestra 4 nodos) con una app de prueba en el gateway; con la de A en la unión J2 |
| T120b | Fallos a nivel de Docker/SO: `docker kill`, `kill -9` de un PID en un contenedor, `docker pause`, `tc netem`, desconexión de red + runner de escenarios (carga + fallo + recolección) | §3.4, §3.7 | Cada fallo se lanza con un comando y queda registrado con su marca de tiempo |
| T121a | Harness de experimentos: generador de carga (N clientes), recolección en `runs/<id>/`, análisis (p50/p95/p99, % parciales, recall@k, checker de mezclas de E3) | §3.5–§3.8 | `make exp SCENARIO=e2` produce un `report.md` con tablas (primero contra el stub) |
| T121b | Ejecutar E1–E3 y redactar el registro (**con A**) | J3 | `fase1_ray/04_experimento_inicial.md` completo |
| T126 | README raíz: qué es, cómo levantarlo, tests, experimentos | — | Alguien nuevo lo levanta siguiendo el README |
| T215 | Failover del cliente entre gateways (extiende el SDK) | §3.1 | Test: con un gateway caído el cliente sigue |
| T222 | Docker Compose de F2: 5 nodos, cada contenedor con sus procesos y volúmenes | §3.3 (versión F2, se acuerda al empezar F2) | Se levantan los 5 nodos con binarios de prueba |
| T223 | Scripts de inyección de fallos de F2 (por nodo y por proceso, partición, retraso) | T120b | Reutiliza T120b con la topología de 5 nodos |
| T224 | Ejecutar los tests de contrato contra F2 | J4 | Verdes |
| T226 | Checker de historias: confirmadas nunca perdidas, RYW, sin resurrecciones | §3.6 | Detecta violaciones inyectadas en historias sintéticas |
| T236 | Revisión cruzada del informe: cada afirmación tiene evidencia | — | Lista de huecos resuelta |
| T301 | Baseline F0 de F1 y F2 con el mismo workload | T121a | Tabla de throughput y p50/p95/p99 |
| T302 | Ejecutar la campaña mínima común (**con A**) | T223 | Registro experimental completo |
| T305 | Mediciones de la comparación Ray/nativo | T301 | Tabla de las 11 dimensiones con datos |
| T307 | Reproducibilidad final desde una máquina limpia | — | `make` de cero funciona |

**Orden sugerido para B en la Fase 1:** T005b → T010 → T119b → T116 → T119 → T118 → T121a →
T120b → T009/T126. Todo esto se puede hacer **sin el código de A**.

---

## 2. Propiedad de carpetas (para no pisarse)

| Dueño | Carpetas / ficheros |
|---|---|
| **A** | `src/dvs/core/`, `src/dvs/coordinator/`, `src/dvs/ray_impl/`, `src/dvs/api/`, `src/dvs/native/` (F2), `src/dvs/config.py`, `src/dvs/obs.py`, `proto/`, `tests/unit/`, `tests/integration/` |
| **B** | `src/dvs/client/`, `tools/stub_server/`, `experiments/`, `deploy/`, `tests/contract/`, `ai_use.yaml`, `README.md` |
| **Compartidos** (cambio avisado en el PR) | `pyproject.toml`, `Makefile` (cada uno con su sección de targets), `.gitignore`, `docs/fase1_ray/01_contrato_api.md` (**solo se cambia si los dos están de acuerdo**) |

El andamiaje (T008: `pyproject.toml`, estructura de carpetas, `Makefile`, ruff, pytest) lo hace
**A primero** y lo sube. B parte de ahí.

**Puesta en marcha** (Ubuntu 24.04):

```bash
sudo apt install -y python3.12-venv      # una vez, para que exista el venv con pip
make install                             # crea .venv e instala el paquete con extras dev + ray
make check                               # lint + mypy + tests unitarios
make help                                # lista los targets, con su dueño
```

- **Versiones:** Python 3.12; **Ray fijado a 2.60.1** (la imagen Docker de B debe usar la
  misma).
- Cada target pendiente del `Makefile` falla diciendo qué tarea lo implementa.
- B rellena los targets de su sección: `data`, `stub`, `test-contract`, `up-ray`, `down-ray` y
  `exp`.

---

## 3. Acuerdos de interfaz (lo que no se cambia sin avisar)

### 3.1 API HTTP

- **Contrato:** `docs/fase1_ray/01_contrato_api.md`. Es la única interfaz entre el SDK, los
  tests y el sistema.
- **Variables de entorno:**
  - `DVS_BASE_URL` (por defecto `http://localhost:8080`; puede ser una lista separada por comas
    para el failover del cliente).
  - `DVS_API_KEY`.
- **Colecciones de prueba:** `demo` (dim 128, L2) y `demo_cos` (dim 128, coseno).
- **Claves de prueba** (solo desarrollo; en la configuración se guarda su sha256):

| Clave | Permisos |
|---|---|
| `dev-key-rw` | lectura y escritura en `demo` y `demo_cos` |
| `dev-key-ro` | solo lectura en `demo` |
| `dev-admin` | `/v1/admin/status` |

### 3.2 Qué pueden y qué no pueden asumir los tests de contrato

- **Pueden asumir:** solo lo que dice el contrato.
- **No pueden asumir:** el número de shards ni el nombre de las réplicas.
- Los tests que necesitan fallos llevan la marca `@pytest.mark.faults` y se saltan si no hay
  inyector disponible.
- Los tests de visibilidad (`eventual` frente a RYW/`strong`) solo comprueban lo que el contrato
  garantiza. Ejemplo: RYW con token **siempre** ve la escritura; `eventual` **puede** no verla.

### 3.3 Arranque del sistema (lo que necesita el Docker Compose de B)

| Pieza | Acuerdo |
|---|---|
| Imagen base | Imagen con **Ray 2.60.1** y Python 3.12 (p. ej. `rayproject/ray:2.60.1-py312-cpu`; B comprueba que el tag existe). Debe coincidir con `pyproject.toml` |
| Paquete | `pip install -e .` del repo dentro de la imagen |
| Ray head | `ray start --head --port=6379 --num-cpus=0 --dashboard-host=0.0.0.0 --block` (dashboard en :8265) |
| Workers (×3) | `ray start --address=ray-head:6379 --num-cpus=8 --block` |
| Gateway | `DVS_RAY_ADDRESS=ray://ray-head:10001 uvicorn dvs.api.main_ray:app --host 0.0.0.0 --port 8080` vía Ray Client; no ejecuta `ray start` (T118 comprueba cuatro nodos: head + 3 workers) |
| Configuración | Variable `DVS_CONFIG` = ruta del YAML (A entrega `deploy/ray/config.ray.yaml`; B lo monta) |
| Evidencia | Volumen `./runs` montado en `/runs` en todos los contenedores; `DVS_RUN_DIR=/runs/<run_id>` |
| Recursos | `shm_size: 2gb` en todos los contenedores Ray; `cap_add: [NET_ADMIN]` en los workers (para `tc netem`) |
| Salud | `GET /v1/health` → 200 cuando el sistema está listo (el compose espera a esto) |

### 3.4 Ganchos de fallos dentro de Ray (los implementa A en T120a; B los invoca)

Se ejecutan desde el contenedor gateway, que tiene acceso al clúster Ray:

```
python -m dvs.ray_impl.faults info                                         # JSON: réplicas con shard, rol, node_id, pid, contenedor
python -m dvs.ray_impl.faults delay --replica s1/r2 --ms 200 --mode block  # block = CPU lenta; async = espera sin bloquear
python -m dvs.ray_impl.faults clear --replica s1/r2
python -m dvs.ray_impl.faults kill  --replica s1/r2 [s1/r0 ...] [--no-restart]
```

B usa `info` para saber qué PID matar con `docker exec <worker> kill -9 <pid>`.

### 3.5 Formato del dataset (lo produce B en T010)

```
data/<nombre>/
  manifest.json          # {seed, n, dim, n_clusters, metric, n_queries, k_gt}
  vectors.npy            # float32 [n, dim]
  ids.txt                # un id por línea, mismo orden que vectors.npy
  metadata.jsonl         # un objeto por línea, mismo orden
  queries.npy            # float32 [n_queries, dim]
  gt_ids.npy, gt_dist.npy  # int64 / float64 [n_queries, k_gt]: top-k exacto (sin filtro), por fuerza bruta en float64
```

- **Metadatos:** `{"cat": "c0".."c9", "price": float 0..1000, "tags": ["t0".."t19"] (1–3), "ver": int}`.
- **Distancias** del ground truth: euclídea para L2 y `1 − cos` para coseno, igual que el
  contrato. Desempate por id.
- `data/` está en `.gitignore`: se regenera con `make data`.

### 3.6 Historias del cliente (las define B en el SDK)

Un fichero JSONL por cliente. Por operación:

```json
{"client_id": "c3", "op": "upsert|insert|update|delete|get|search", "collection": "demo",
 "id": "...", "request_id": "...", "t_inv_mono": 0.0, "t_resp_mono": 0.0, "t_inv_wall": "...",
 "outcome": "ok|error|timeout", "http_status": 200, "error_code": null,
 "token": {"shard": 1, "seq": 42}, "result_summary": {...}}
```

### 3.7 Logs del servidor (los produce A; los consume el análisis de B)

- JSONL en `${DVS_RUN_DIR}/logs/<componente>-<pid>.jsonl`.
- **Campos:** `ts`, `mono`, `node_id`, `pid`, `component`, `replica_id`, `role`, `incarnation`,
  `shard`, `seq`, `commit_seq`, `applied_seq`, `request_id`, `query_id`, `event`,
  `latency_ms`, `status`.
- **Eventos:**
  - Escritura: `write.received`, `write.committed`, `write.applied`, `write.timeout`,
    `write.dedup_hit`.
  - Búsqueda: `search.fanout`, `search.shard_result`, `search.hedge`,
    `search.stale_redirect`, `search.done`.
  - Réplica: `replica.started`, `replica.not_ready`.
  - Admisión: `admission.rejected`.

### 3.8 Métricas y estado (los expone A)

```json
GET /v1/metrics
{"ops": {"search": {"count": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0}, "write": {...}, "get": {...}},
 "shards": {"0": {"count": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0}},
 "counters": {"partial": 0, "admission_rejected": 0, "hedges": 0, "stale_redirects": 0},
 "replication_lag": {"s0/r1": 0}}

GET /v1/admin/status
{"map_version": 1,
 "replicas": [{"replica_id": "s0/r1", "shard": 0, "role": "primary|backup", "ready": true,
               "node_id": "...", "pid": 0, "incarnation": "...", "applied_seq": 0, "commit_seq": 0}]}
```

### 3.9 Estructura de un run de experimento (la define B)

```
runs/<run_id>/
  run.json                      # escenario, parámetros, semilla, commit de git
  client/latencies.csv          # op, t_inv, t_resp, latency_ms, status, partial, coverage
  client/history-*.jsonl
  faults.jsonl                  # qué fallo, a quién, cuándo (mono + pared)
  logs/*.jsonl                  # logs del servidor
  status_before.json, status_after.json, metrics_*.json
  report.md                     # tablas generadas por el análisis
```

---

## 4. Puntos de unión

| Unión | Cuándo | Qué se junta | Prueba de que funciona |
|---|---|---|---|
| **J1** | A tiene la API corriendo en local (`make run-ray-local`) | SDK + tests de contrato de B contra el sistema real | `DVS_BASE_URL=http://localhost:8080 make test-contract` verde |
| **J2** | J1 + compose de B listo | Imagen + compose de B con la app y la configuración de A | `make up-ray` + `ray status` con 4 nodos + logs con réplicas en nodos distintos |
| **J3** | J2 + ganchos T120a | Harness y fallos de B sobre el despliegue real | `make exp SCENARIO=e1/e2/e3` con evidencia en `runs/` |
| **J4** (F2) | Nodos nativos de A | Compose de 5 nodos y fallos de B | Tests de contrato verdes contra F2 + un par de fallos |

Antes de J1, B trabaja contra el **stub** (T119b). El stub implementa el contrato con un
diccionario en memoria y fuerza bruta, sin shards reales: no simula fallos ni parciales.

---

## 5. Forma de trabajo con git

- Cada uno trabaja en su rama (`ronald_feature`, `<compañero>_feature`) y se integra a `main`
  con PR pequeños y frecuentes.
- **Cada PR lo revisa el otro.** Así los dos conocen todo el código para la defensa.
- Si algo de la §3 necesita cambiar, se propone en un PR que toque solo ese acuerdo (y el
  contrato, si aplica), y el otro lo aprueba antes de implementarlo.
- Al cerrar una tarea, se marca `[x]` en `PLAN_DE_TRABAJO.md` dentro del mismo PR.
