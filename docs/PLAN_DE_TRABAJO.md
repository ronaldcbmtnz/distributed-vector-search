# Plan de trabajo — Motor distribuido de búsqueda vectorial

> Lista **ordenada** de todo lo que hay que hacer hasta la defensa. Se marca `[x]` al terminar
> cada tarea, siguiendo la definición de "hecho": tests verdes, docs al día y commit.
>
> - `(dep: Tnn)` indica que la tarea depende de otra.
> - `[P]` indica que puede hacerse en paralelo con la anterior.
> - El calendario oficial de entregas está en [`README.md`](README.md).
>
> **Responsables** (detalle y acuerdos de interfaz en [`REPARTO_TAREAS.md`](REPARTO_TAREAS.md)):
> - **[A]** Sistema: núcleo, replicación, Ray, Raft, API (Ronald).
> - **[B]** Plataforma y verificación: cliente, datos, tests de contrato, Docker, fallos,
>   experimentos (compañero).
> - **[A+B]** Los dos.

## Hitos

| Hito | Criterio de salida |
|---|---|
| H0 — Decisiones cerradas | ✅ Decisiones de F1 cerradas; constitución, spec y plan técnico de F1 aprobados (C1 queda para F2) |
| H1 — Prototipo Ray completo | Tests de contrato verdes en un clúster Ray de varios nodos |
| H2 — **Entrega 1** (Ray) | Artefactos F1 + tag `v1-ray` + issue en GitHub |
| H3 — Núcleo de consenso probado | Tests deterministas de Raft verdes (elección, replicación, persistencia) |
| H4 — Sistema nativo funcional | Tests de contrato verdes contra F2 en 5 nodos |
| H5 — **Entrega 2** (informe sin Ray) | Informe de arquitectura completo |
| H6 — Dos fallos verificados | Escenarios de pares de fallos con evidencia |
| H7 — **Defensa** | Campaña F3 + comparación + ensayo individual |

---

## Fase 0 — Preparación

- [x] T001 [A] — Leer toda la orientación del proyecto y extraer requisitos
- [x] T002 [A] — Documento introductorio problema → decisiones · hecho cuando: `docs/00_introduccion.md`
- [x] T003 [A] — Plan de trabajo y esqueletos de entregables · hecho cuando: `docs/` con índice
- [x] T004a [A] — Cerrar las decisiones estructurales:
  - D05 = Raft por shard, RF = 5.
  - D18 = Python.
  - C2 = replicación obligatoria en F1.
  - C3 = colecciones por configuración.
- [x] T004b [A] — Cerrar las decisiones menores de F1: C4 = métricas L2 + coseno; C5 = dedup sin expiración
- [ ] T004c [A] — Cerrar C1 (ventana de failover acotada): solo afecta a F2 (ver T007)
- [x] T005 [A] — Aprobar constitución, spec del contrato y plan técnico de F1
- [x] T005b [A+B] — Instalar Docker Engine + Compose en la máquina de cada uno · hecho cuando: `docker compose version` responde sin `sudo` [P]
- [ ] T006 [A+B] — Confirmar equipo: nombres completos, grupos y reparto de áreas · hecho cuando: tabla en `docs/README.md` y nombre en `REPARTO_TAREAS.md`
- [ ] T007 [A] — Preguntar al profesor (si hace falta): ¿una ventana acotada sin escrituras durante el failover cuenta como "mantener la funcionalidad"? [P]
- [x] T008 [A] — Andamiaje del repo. **Va primero: B parte de aquí** (dep: T005)
  - `pyproject.toml` con la versión de Ray fijada.
  - Estructura `src/`, `tests/`, `experiments/`, `deploy/`, `tools/`.
  - `ruff`, `pytest`, `Makefile` con secciones A/B.

  Hecho cuando: `make test` pasa con un test vacío.
- [x] T009 [B] — `ai_use.yaml` en la raíz, siguiendo el formato de la metodología, y aviso de terceros en el `README.md` [P]
- [x] T010 [B] — Generador de datos (formato en REPARTO §3.5) · hecho cuando: `make data` reproduce los mismos ficheros [P]
  - Vectores sintéticos con semilla (gaussianas por cluster).
  - Metadatos sintéticos.
  - Ground truth por fuerza bruta.

## Fase 1 — Prototipo con Ray

### F1-A · Núcleo puro de shard, sin red (reutilizable en F2) — [A]

- [x] T101 [A] — Hash estable `(colección, id) → shard` (blake2b) · cubre: D01 · hecho cuando: test de estabilidad entre procesos y de uniformidad (dep: T008)
- [x] T102 [A] — Validación de entrada: dimensión, NaN/Inf, metadatos, límites, `k`, complejidad del filtro · cubre: REQ-1.6, REQ-2.6 [P]
- [x] T103 [A] — Lenguaje de filtros (igualdad, `$in`, rango, AND) y su evaluación · cubre: REQ-2.3 [P]
- [x] T104 [A] — `ShardCore`: índice exacto + metadatos · cubre: REQ-1.1–1.5, REQ-1.7, REQ-2.5 (dep: T102, T103)
  - Operaciones: upsert, insert y update condicionales, delete y get.
  - Búsqueda top-k con pre-filtrado.
- [x] T105 [A] — Tabla de deduplicación por `request_id`, incluido el conflicto por contenido distinto · cubre: REQ-5.1–5.3 (dep: T104)
- [x] T106 [A] — Merge determinista de top-k locales por `(dist, id)` · cubre: REQ-2.1, 2.2, 2.4 · hecho cuando: test de propiedad frente a fuerza bruta global y test de permutaciones [P]
- [x] T107 [A] — Contadores de posición: `applied_index` por shard y token de escritura · cubre: REQ-4.4 (dep: T104)

### F1-B · Sistema sobre Ray — [A]

- [x] T110 [A] — Actor `ShardReplica` que envuelve `ShardCore`; named actors; `max_restarts` configurable · cubre: D12 (dep: T104–T107)
- [x] T111 [A] — Actor `Catalog` · cubre: C3 (dep: T110)
  - Carga las colecciones (dimensión, métrica, permisos) desde el **fichero de configuración**.
  - Publica el mapa de shards versionado: shard → primario + backups.
- [x] T112 [A] — Coordinador scatter-gather · cubre: REQ-3.1–3.5, D09 (dep: T110)
  - Plazo global y timeout por intento.
  - Hedge si RF > 1.
  - `partial` / `missing_shards` / `coverage` y `allow_partial`.
  - Cancelación de lo pendiente.
- [x] T113 [A] — Camino de escritura: enrutar al responsable del shard, `request_id` y token · cubre: HU-1, HU-5 (dep: T110)
- [x] T114 [A] — **Replicación primario → backups en memoria (obligatoria, C2)** · cubre: HU-4, REQ-6.1 · hecho cuando: test que muestra la ventana `stale` con `eventual` y su desaparición con RYW/`strong` (dep: T113)
  - RF configurable, por defecto 3. Confirmación con W acks (por defecto mayoría).
  - Los niveles `eventual`, `read_your_writes` y `strong` funcionan.
  - Respuesta `stale` sin espera.
  - Sin persistencia ni failover: la tolerancia a fallos no aplica en F1.
- [x] T115 [A] — API REST con FastAPI · cubre: contrato (dep: T112, T113)
  - Todas las operaciones del contrato.
  - Códigos de error del contrato.
  - API key, permisos por colección y semáforo de admisión con 429.
  - `/v1/metrics` y `/v1/admin/status` (REPARTO §3.8).
- [x] T117 [A] — Logs JSON estructurados (REPARTO §3.7) y métricas p50/p95/p99 en el gateway · cubre: HU-10 [P]

### F1-B' · Cliente, contrato y despliegue — [B] (independiente de F1-A/F1-B hasta J1)

- [x] T119b [B] — **Servidor simulado del contrato** en `tools/stub_server/` (en memoria, sin distribución) para desarrollar sin depender de A · hecho cuando: los tests de contrato sin fallos pasan contra él (dep: T008)
- [x] T116 [B] — SDK/CLI del cliente · cubre: REQ-7.7, 8.3, 10.4 · hecho cuando: tests del SDK verdes contra el stub (dep: T119b)
  - Genera `request_id`.
  - Reintentos con backoff y jitter.
  - Lista de puntos de entrada.
  - Tokens RYW.
  - Registro de historias (REPARTO §3.6).
- [x] T119 [B] — Tests de contrato (pytest + httpx) contra `DVS_BASE_URL`, **independientes de Ray** · cubre: todos los P0 de la spec · hecho cuando: verdes contra el stub y, en J1, contra el sistema real (dep: T119b)
  - Camino feliz.
  - Errores.
  - Parciales (`@pytest.mark.faults`).
  - Duplicados.
  - Concurrencia.
- [x] T118 [B] — Docker Compose del clúster Ray (REPARTO §3.3): head + 3 workers + gateway, `make up-ray` · cubre: REQ-11.1 · hecho cuando: `ray status` muestra 4 nodos; con la app real en J2 los logs muestran réplicas en nodos distintos (dep: T005b)

### F1-C · Experimento y documentos de la entrega

- [x] T120a [A] — Ganchos de fallos dentro de Ray: CLI `python -m dvs.ray_impl.faults` con `info`/`delay`/`clear`/`kill` (REPARTO §3.4) (dep: T110)
- [x] T120b [B] — Fallos a nivel de Docker/SO + runner de escenarios (carga + fallo + recolección, con registro en `faults.jsonl`) [P]
  - Fallos: `docker kill`, `kill -9` de un PID, `docker pause`, `tc netem`, desconexión de red, `ray-delay` y `ray-kill-shard`; todos verificados contra el Compose.
- [x] T121a [B] — Harness de experimentos: generador de carga, estructura `runs/<id>/` (REPARTO §3.9) y análisis · hecho cuando: `make exp SCENARIO=e2` genera `report.md` (contra el stub primero) (dep: T010, T116)
  - Análisis: p50/p95/p99, % parciales, recall@k frente a ground truth y checker de mezclas para E3.
- [ ] T121b [A+B] — (ejecutado: evidencia en `experiments/results/f1/`; falta redactar `04_experimento_inicial.md`) Ejecutar el experimento inicial y registrarlo con la plantilla de 10 campos · hecho cuando: `docs/fase1_ray/04_experimento_inicial.md` relleno con evidencia (logs/CSV) (dep: J3 = T118 + T120a + T120b + T121a)
  - E1: crash de actor.
  - E2: shard lento → parciales.
  - E3: escrituras y búsquedas concurrentes.
- [ ] T122 [A] — Redactar el contrato de comportamiento/API · hecho cuando: `docs/fase1_ray/01_contrato_api.md` final (dep: T115) [P]
- [ ] T123 [A] — Diagrama de arquitectura F1 (componentes lógicos + runtime Ray) · hecho cuando: `docs/fase1_ray/02_arquitectura_ray.md` [P]
- [ ] T124 [A] — Mapa de dependencias del middleware, con lo **observado** en T121b · hecho cuando: `docs/fase1_ray/03_mapa_dependencias_middleware.md` (dep: T121b)
- [ ] T125 [A] — Responder la puerta de salida (5 preguntas) y congelar el contrato · hecho cuando: `docs/fase1_ray/README.md` §Puerta (dep: T121b–T124)
- [x] T126 [B] — README raíz: qué es, cómo levantarlo, cómo correr tests y experimentos, aviso de terceros, `ai_use` (dep: T118)
- [ ] T127 [A+B] — (verificado: `make check`, integración y contrato verdes, demo = batería de T121b; falta el tag `v1-ray` tras el commit) Revisión final: `make test` limpio, ensayo de demo, tag `v1-ray` (dep: todo F1)
- [ ] T128 [A+B] — **ENTREGA 1:** issue en GitHub (tema, nombre completo + grupo de cada integrante, enlace)

## Fase 2 — Sistema nativo sin Ray + informe

> Las tareas finas de esta fase se derivan con SDD (`plan.md` / `tasks.md` por feature) al
> cerrar la Fase 1. Aquí va el esqueleto en orden. Decisiones de base: D05 = Raft por shard
> con RF = 5, D18 = Python.

### F2-A · Cimientos — [A]

- [ ] T201 [A] — Registro de decisiones inicial (tabla Requisito | F1 Ray | Alternativas | Diseño | Razón) · hecho cuando: `informe_arquitectura/06_registro_decisiones.md` v1
- [ ] T202 [A] — Definición protobuf de los servicios internos (Search, Write, Get, Status, RequestVote, AppendEntries) con campo de versión [P]
- [ ] T203 [A] — Configuración de clúster (nodos, placement, S, RF, timeouts) y su carga/validación [P]
- [ ] T204 [A] — Log persistente: append, `fsync`, lectura y truncado; persistencia de `currentTerm`/`votedFor` · hecho cuando: test de `kill -9` simulado (escritura parcial al final del archivo)
- [ ] T205 [A] — Raft: elección de líder (timeouts aleatorios, votos persistidos, regla de log más actualizado) sobre **red simulada determinista** · cubre: REQ-7.5
- [ ] T206 [A] — Raft: replicación (AppendEntries, `prevLogIndex/Term`, avance de `commitIndex` solo con entradas del término actual) · cubre: REQ-7.3 (dep: T205)
- [ ] T207 [A] — Checker de invariantes de Raft en los tests: Election Safety, Log Matching, Leader Completeness, State Machine Safety (dep: T206)

### F2-B · Datos y caminos de lectura/escritura — [A]

- [ ] T210 [A] — Máquina de estados = `ShardCore` + dedup, aplicada desde el log en un solo hilo · cubre: D12, REQ-5.4
- [ ] T211 [A] — Servidor de nodo de almacenamiento: S grupos Raft, servidor gRPC, cola de comandos acotada (dep: T210, T202)
- [ ] T212 [A] — Lecturas por nivel: `eventual`, `read_your_writes` (`stale` si `applied < token`) y `strong` (ReadIndex) · cubre: HU-4
- [ ] T213 [A] — Snapshots de la máquina de estados y arranque desde snapshot + log; estado "lista" · cubre: REQ-7.4
- [ ] T214 [A] — Gateway nativo · cubre: HU-3, D15 (dep: T211)
  - Caché de líderes con pista `not_leader`.
  - Scatter-gather asyncio con plazo y hedge.
  - Encaminamiento de escrituras.
  - Admisión.
  - Misma API REST.
- [ ] T215 [B] — Cliente con failover entre gateways (extiende el SDK de F1)

### F2-C · Operación, seguridad y fallos

- [ ] T220 [A] — Seguridad: API keys, permisos por colección, límites y rate limit · cubre: HU-9
- [ ] T221 [A] — Observabilidad: logs con term/index/incarnation, endpoint de métricas y de estado por nodo · cubre: HU-10
- [ ] T222 [B] — Docker Compose con 5 nodos (un contenedor por nodo con sus procesos) y volúmenes · cubre: REQ-11.1
- [ ] T223 [B] — Scripts de inyección de fallos de F2: por nodo y por proceso, partición y retraso (reutiliza T120b) · cubre: REQ-11.3
- [ ] T224 [B] — Tests de contrato de F1 **sin cambios** contra F2 · hecho cuando: verdes (dep: J4)
- [ ] T225 [A+B] — Primeras pruebas de pares de fallos: 2 nodos, líder + follower del mismo shard, gateway + nodo
- [ ] T226 [B] — Checker de historias: escrituras confirmadas nunca perdidas, RYW y nada de resurrecciones [P]

### F2-D · Informe de arquitectura — [A], con revisión de [B]

- [ ] T230 [A] — `01_servicio_y_garantias.md`
- [ ] T231 [A] — `02_arquitectura_sin_ray.md` (diagrama nodos/procesos/canales + tabla "lo que hacía Ray → quién lo hace ahora")
- [ ] T232 [A] — `03_fichas_componentes.md` (una ficha por componente)
- [ ] T233 [A] — `04_operacion_extremo_a_extremo.md` (una escritura y una búsqueda RYW, mensaje a mensaje)
- [ ] T234 [A] — `05_dos_fallos_y_recuperacion.md` (clases de pares + un par representativo traza a traza)
- [ ] T235 [A] — `06_registro_decisiones.md` final
- [ ] T236 [B] — Revisión cruzada: cada afirmación del informe apunta a un test o experimento
- [ ] T237 [A+B] — **ENTREGA 2:** informe (exportar a PDF si se pide) y comentario o issue en GitHub

## Fase 3 — Verificación, comparación y defensa

- [ ] T301 [B] — F0 baseline: throughput y p50/p95/p99 de F1 y F2 con el mismo workload
- [ ] T302 [A+B] — Campaña mínima común (8 puntos) con registro experimental completo
- [ ] T303 [A] — Campaña específica del proyecto · cubre: `fase3_verificacion/01` §P
  - Shard no disponible y política de parciales.
  - recall@k frente a cobertura.
  - Pares de fallos.
  - Líder zombi.
  - Visibilidad bajo retraso.
- [ ] T304 [A] — Análisis de garantías: safety frente a liveness y qué evidencia sostiene cada una
- [ ] T305 [B] — Comparación Ray/nativo en las 11 dimensiones de la metodología
- [ ] T306 [A+B] — Preparación de la defensa · hecho cuando: cada integrante lo hace solo
  - Predecir fallos antes de inyectarlos.
  - Localizar cada garantía en el código.
  - Ensayo de modificación pequeña.
- [ ] T307 [B] — Actualizar `ai_use.yaml`, README y reproducibilidad final (`make` de cero en una máquina limpia)
