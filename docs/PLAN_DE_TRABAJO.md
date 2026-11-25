# Plan de trabajo — Motor distribuido de búsqueda vectorial

> Lista **ordenada** de todo lo que hay que hacer hasta la defensa. Se marca `[x]` al terminar
> cada tarea, siguiendo la definición de "hecho": tests verdes, docs al día y commit.
>
> - `(dep: Tnn)` indica que la tarea depende de otra.
> - `[P]` indica que puede hacerse en paralelo con la anterior.
> - El calendario oficial de entregas está en [`README.md`](README.md).

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

- [x] T001 — Leer toda la orientación del proyecto y extraer requisitos
- [x] T002 — Documento introductorio problema → decisiones · hecho cuando: `docs/00_introduccion.md`
- [x] T003 — Plan de trabajo y esqueletos de entregables · hecho cuando: `docs/` con índice
- [x] T004a — Cerrar las decisiones estructurales:
  - D05 = Raft por shard, RF = 5.
  - D18 = Python.
  - C2 = replicación obligatoria en F1.
  - C3 = colecciones por configuración.
- [x] T004b — Cerrar las decisiones menores de F1: C4 = métricas L2 + coseno; C5 = dedup sin expiración
- [ ] T004c — Cerrar C1 (ventana de failover acotada): solo afecta a F2 (ver T007)
- [x] T005 — Aprobar constitución, spec del contrato y plan técnico de F1
- [ ] T005b — Instalar Docker Engine + Compose en la máquina de desarrollo · hecho cuando: `docker compose version` responde sin `sudo` [P]
- [ ] T006 — Confirmar equipo: nombres completos, grupos y reparto de áreas · hecho cuando: tabla en `docs/README.md`
- [ ] T007 — Preguntar al profesor (si hace falta): ¿una ventana acotada sin escrituras durante el failover cuenta como "mantener la funcionalidad"? [P]
- [ ] T008 — Andamiaje del repo (dep: T005)
  - `pyproject.toml`, estructura `src/`, `tests/`, `experiments/`, `deploy/`.
  - `ruff`, `pytest`, `Makefile` con `make test`.
  - CI opcional.

  Hecho cuando: `make test` pasa con un test vacío.
- [ ] T009 — `ai_use.yaml` en la raíz, siguiendo el formato de la metodología, y aviso de terceros en el `README.md` [P]
- [ ] T010 — Generador de datos · hecho cuando: `make data` reproduce los mismos ficheros [P]
  - Vectores sintéticos con semilla (gaussianas por cluster).
  - Metadatos sintéticos.
  - Ground truth por fuerza bruta.

## Fase 1 — Prototipo con Ray

> El detalle técnico de cada tarea sale del plan técnico de la Fase 1. Esta lista se ajusta al
> aprobar ese plan.

### F1-A · Núcleo puro de shard, sin red (reutilizable en F2)

- [ ] T101 — Hash estable `(colección, id) → shard` (blake2b) · cubre: D01 · hecho cuando: test de estabilidad entre procesos y de uniformidad (dep: T008)
- [ ] T102 — Validación de entrada: dimensión, NaN/Inf, metadatos, límites, `k`, complejidad del filtro · cubre: REQ-1.6, REQ-2.6 [P]
- [ ] T103 — Lenguaje de filtros (igualdad, `$in`, rango, AND) y su evaluación · cubre: REQ-2.3 [P]
- [ ] T104 — `ShardCore`: índice exacto + metadatos · cubre: REQ-1.1–1.5, REQ-1.7, REQ-2.5 (dep: T102, T103)
  - Operaciones: upsert, insert y update condicionales, delete y get.
  - Búsqueda top-k con pre-filtrado.
- [ ] T105 — Tabla de deduplicación por `request_id`, incluido el conflicto por contenido distinto · cubre: REQ-5.1–5.3 (dep: T104)
- [ ] T106 — Merge determinista de top-k locales por `(dist, id)` · cubre: REQ-2.1, 2.2, 2.4 · hecho cuando: test de propiedad frente a fuerza bruta global y test de permutaciones [P]
- [ ] T107 — Contadores de posición: `applied_index` por shard y token de escritura · cubre: REQ-4.4 (dep: T104)

### F1-B · Sistema sobre Ray

- [ ] T110 — Actor `ShardReplica` que envuelve `ShardCore`; named actors; `max_restarts` configurable · cubre: D12 (dep: T104–T107)
- [ ] T111 — Actor `Catalog` · cubre: C3 (dep: T110)
  - Carga las colecciones (dimensión, métrica, permisos) desde el **fichero de configuración**.
  - Publica el mapa de shards versionado: shard → primario + backups.
- [ ] T112 — Coordinador scatter-gather · cubre: REQ-3.1–3.5, D09 (dep: T110)
  - Plazo global y timeout por intento.
  - Hedge si RF > 1.
  - `partial` / `missing_shards` / `coverage` y `allow_partial`.
  - Cancelación de lo pendiente.
- [ ] T113 — Camino de escritura: enrutar al responsable del shard, `request_id` y token · cubre: HU-1, HU-5 (dep: T110)
- [ ] T114 — **Replicación primario → backups en memoria (obligatoria, C2)** · cubre: HU-4, REQ-6.1 · hecho cuando: test que muestra la ventana `stale` con `eventual` y su desaparición con RYW/`strong` (dep: T113)
  - RF configurable, por defecto 3. Confirmación con W acks (por defecto mayoría).
  - Los niveles `eventual`, `read_your_writes` y `strong` funcionan.
  - Respuesta `stale` sin espera.
  - Sin persistencia ni failover: la tolerancia a fallos no aplica en F1.
- [ ] T115 — API REST con FastAPI · cubre: contrato (dep: T112, T113)
  - Todas las operaciones del contrato.
  - Códigos de error del contrato.
  - API key, permisos por colección y semáforo de admisión con 429.
- [ ] T116 — SDK/CLI del cliente · cubre: REQ-7.7, 8.3, 10.4 [P]
  - Genera `request_id`.
  - Reintentos con backoff y jitter.
  - Lista de puntos de entrada.
  - Registro de historias.
- [ ] T117 — Logs JSON estructurados con `query_id`/`request_id` y métricas p50/p95/p99 · cubre: HU-10 [P]
- [ ] T118 — Despliegue multi-nodo: Docker Compose con Ray head + workers, réplicas repartidas y `make up-ray` · cubre: REQ-11.1 · hecho cuando: los logs muestran actores en nodos distintos (dep: T115)
- [ ] T119 — Tests de contrato (pytest) contra la API, **independientes de Ray** · cubre: todos los P0 de la spec (dep: T115)
  - Camino feliz.
  - Errores.
  - Parciales.
  - Duplicados.
  - Concurrencia.

### F1-C · Experimento y documentos de la entrega

- [ ] T120 — Inyección de fallos sobre Ray: matar el proceso de un actor, retraso artificial en un shard y carga concurrente · cubre: experimento inicial (dep: T118)
- [ ] T121 — Ejecutar el experimento inicial y registrarlo con la plantilla de 10 campos · hecho cuando: `docs/fase1_ray/04_experimento_inicial.md` relleno con evidencia (logs/CSV) (dep: T120)
  - E1: crash de actor.
  - E2: shard lento → parciales.
  - E3: escrituras y búsquedas concurrentes.
- [ ] T122 — Redactar el contrato de comportamiento/API · hecho cuando: `docs/fase1_ray/01_contrato_api.md` final (dep: T115) [P]
- [ ] T123 — Diagrama de arquitectura F1 (componentes lógicos + runtime Ray) · hecho cuando: `docs/fase1_ray/02_arquitectura_ray.md` [P]
- [ ] T124 — Mapa de dependencias del middleware, con lo **observado** en T121 · hecho cuando: `docs/fase1_ray/03_mapa_dependencias_middleware.md` (dep: T121)
- [ ] T125 — Responder la puerta de salida (5 preguntas) y congelar el contrato · hecho cuando: `docs/fase1_ray/README.md` §Puerta (dep: T121–T124)
- [ ] T126 — README raíz: qué es, cómo levantarlo, cómo correr tests y experimento, aviso de terceros, `ai_use` (dep: T118)
- [ ] T127 — Revisión final: `make test` limpio, ensayo de demo, tag `v1-ray` (dep: todo F1)
- [ ] T128 — **ENTREGA 1:** issue en GitHub (tema, nombre completo + grupo de cada integrante, enlace)

## Fase 2 — Sistema nativo sin Ray + informe

> Las tareas finas de esta fase se derivan con SDD (`plan.md` / `tasks.md` por feature) al
> cerrar la Fase 1. Aquí va el esqueleto en orden. Decisiones de base: D05 = Raft por shard
> con RF = 5, D18 = Python.

### F2-A · Cimientos

- [ ] T201 — Registro de decisiones inicial (tabla Requisito | F1 Ray | Alternativas | Diseño | Razón) · hecho cuando: `informe_arquitectura/06_registro_decisiones.md` v1
- [ ] T202 — Definición protobuf de los servicios internos (Search, Write, Get, Status, RequestVote, AppendEntries) con campo de versión [P]
- [ ] T203 — Configuración de clúster (nodos, placement, S, RF, timeouts) y su carga/validación [P]
- [ ] T204 — Log persistente: append, `fsync`, lectura y truncado; persistencia de `currentTerm`/`votedFor` · hecho cuando: test de `kill -9` simulado (escritura parcial al final del archivo)
- [ ] T205 — Raft: elección de líder (timeouts aleatorios, votos persistidos, regla de log más actualizado) sobre **red simulada determinista** · cubre: REQ-7.5
- [ ] T206 — Raft: replicación (AppendEntries, `prevLogIndex/Term`, avance de `commitIndex` solo con entradas del término actual) · cubre: REQ-7.3 (dep: T205)
- [ ] T207 — Checker de invariantes de Raft en los tests: Election Safety, Log Matching, Leader Completeness, State Machine Safety (dep: T206)

### F2-B · Datos y caminos de lectura/escritura

- [ ] T210 — Máquina de estados = `ShardCore` + dedup, aplicada desde el log en un solo hilo · cubre: D12, REQ-5.4
- [ ] T211 — Servidor de nodo de almacenamiento: S grupos Raft, servidor gRPC, cola de comandos acotada (dep: T210, T202)
- [ ] T212 — Lecturas por nivel: `eventual`, `read_your_writes` (`stale` si `applied < token`) y `strong` (ReadIndex) · cubre: HU-4
- [ ] T213 — Snapshots de la máquina de estados y arranque desde snapshot + log; estado "lista" · cubre: REQ-7.4
- [ ] T214 — Gateway nativo · cubre: HU-3, D15 (dep: T211)
  - Caché de líderes con pista `not_leader`.
  - Scatter-gather asyncio con plazo y hedge.
  - Encaminamiento de escrituras.
  - Admisión.
  - Misma API REST.
- [ ] T215 — Cliente con failover entre gateways (reutiliza el SDK de F1)

### F2-C · Operación, seguridad y fallos

- [ ] T220 — Seguridad: API keys, permisos por colección, límites y rate limit · cubre: HU-9
- [ ] T221 — Observabilidad: logs con term/index/incarnation, endpoint de métricas y de estado por nodo · cubre: HU-10
- [ ] T222 — Docker Compose con 5 nodos (un contenedor por nodo con sus procesos) y volúmenes · cubre: REQ-11.1
- [ ] T223 — Scripts de inyección de fallos: `kill`, `docker kill`, `pause`, `tc netem` y desconexión de red · cubre: REQ-11.3
- [ ] T224 — Tests de contrato de F1 **sin cambios** contra F2 · hecho cuando: verdes
- [ ] T225 — Primeras pruebas de pares de fallos: 2 nodos, líder + follower del mismo shard, gateway + nodo
- [ ] T226 — Checker de historias: escrituras confirmadas nunca perdidas, RYW y nada de resurrecciones

### F2-D · Informe de arquitectura

- [ ] T230 — `01_servicio_y_garantias.md`
- [ ] T231 — `02_arquitectura_sin_ray.md` (diagrama nodos/procesos/canales + tabla "lo que hacía Ray → quién lo hace ahora")
- [ ] T232 — `03_fichas_componentes.md` (una ficha por componente)
- [ ] T233 — `04_operacion_extremo_a_extremo.md` (una escritura y una búsqueda RYW, mensaje a mensaje)
- [ ] T234 — `05_dos_fallos_y_recuperacion.md` (clases de pares + un par representativo traza a traza)
- [ ] T235 — `06_registro_decisiones.md` final
- [ ] T236 — Revisión cruzada: cada afirmación del informe apunta a un test o experimento
- [ ] T237 — **ENTREGA 2:** informe (exportar a PDF si se pide) y comentario o issue en GitHub

## Fase 3 — Verificación, comparación y defensa

- [ ] T301 — F0 baseline: throughput y p50/p95/p99 de F1 y F2 con el mismo workload
- [ ] T302 — Campaña mínima común (8 puntos) con registro experimental completo
- [ ] T303 — Campaña específica del proyecto · cubre: `fase3_verificacion/01` §P
  - Shard no disponible y política de parciales.
  - recall@k frente a cobertura.
  - Pares de fallos.
  - Líder zombi.
  - Visibilidad bajo retraso.
- [ ] T304 — Análisis de garantías: safety frente a liveness y qué evidencia sostiene cada una
- [ ] T305 — Comparación Ray/nativo en las 11 dimensiones de la metodología
- [ ] T306 — Preparación de la defensa · hecho cuando: cada integrante lo hace solo
  - Predecir fallos antes de inyectarlos.
  - Localizar cada garantía en el código.
  - Ensayo de modificación pequeña.
- [ ] T307 — Actualizar `ai_use.yaml`, README y reproducibilidad final (`make` de cero en una máquina limpia)
