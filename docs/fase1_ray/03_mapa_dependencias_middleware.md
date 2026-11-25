# Mapa de dependencias del middleware

> 🚧 **Prerrelleno con lo esperado.** Se corrige con lo **observado** en el experimento inicial
> (T124).
> **Qué pide la metodología:** para cada requisito distribuido, qué lo proporciona en la Fase 1
> y qué pregunta abre para el rediseño nativo. La última columna anticipa nuestra respuesta.

| Requisito distribuido | Mecanismo en Fase 1 (Ray) | Pregunta para el rediseño nativo | Respuesta prevista en Fase 2 |
|---|---|---|---|
| Ejecución remota | Llamadas a métodos de actor (`.remote()`) | ¿Qué protocolo explícito hace falta? | gRPC unario gateway → nodo; RPC de Raft entre réplicas |
| Worker con estado | Actor `ShardReplica` (estado en memoria) | ¿Qué proceso posee el estado y cómo se recupera? | Réplica en el proceso del nodo; log persistente + snapshot; puesta al día vía Raft |
| Scheduling / placement | Scheduler de Ray (estrategia SPREAD, recursos) | ¿Hace falta scheduling dinámico? | No: placement estático en la configuración (anti-afinidad de réplicas) |
| Resultado remoto | `ObjectRef` + `ray.get`/`ray.wait` | ¿Basta un job ID + result store? | No hay trabajos largos: respuesta síncrona de la RPC con deadline |
| Membresía / directorio | GCS + actores con nombre | ¿Bootstrap estático, registro, gossip, DHT, coordinador? | Lista estática de nodos + líder por shard vía Raft (pista `not_leader`) |
| Quién manda en cada shard | Fijo (réplica 0) o actor Catalog | ¿Quién decide y cómo se evita el split-brain? | Elección de Raft por shard; término como fencing |
| Reintento | `max_restarts` / `max_task_retries` del runtime | ¿Cuál es la semántica real de reintento? | Reintentos explícitos del cliente/gateway con `request_id`; hedge solo para lecturas |
| Fallo de un worker | `RayActorError`; reinicio **con estado vacío** | ¿Cómo se detecta y qué estado sobrevive? | Timeouts + elección; recuperación desde disco; estado "lista" |
| Esperas acotadas | `ray.wait(timeout=…)`, `ray.cancel` | ¿Cómo se implementan plazos y cancelación? | Deadlines de gRPC + `asyncio.wait` + cancelación |
| Serialización | Pickle/Arrow, transparente | ¿Qué formato y cómo se versiona? | Protobuf; vectores `<f4` en bytes; campo de versión |
| Concurrencia en el estado | Buzón del actor: una llamada a la vez | ¿Cómo se serializa el acceso? | Cola de comandos acotada de la réplica, un hilo |
| Recursos | Recursos lógicos de Ray (`num_cpus`) | ¿Qué restricciones importan realmente? | Límites del contenedor + colas y semáforos acotados |
| Observabilidad | Dashboard y logs de workers de Ray | ¿Qué evidencia debe exponer la versión nativa? | Logs JSON con term/index/IDs, métricas p50/p95/p99, historias + checker |

## Lo que Ray **no** nos da (y por eso es código nuestro desde la Fase 1)

- **Persistencia:** `max_restarts` reinicia el actor con el índice vacío.
- **Consenso y orden entre clientes:** Ray solo ordena las llamadas de un mismo llamador a un
  actor.
- **Idempotencia** de nuestras operaciones (`request_id` + dedup).
- **Política de degradación:** plazos, parciales y formato de respuesta.
- **Merge determinista** y semántica de visibilidad.

## Observaciones del experimento

_(completar con lo observado en T121: qué hizo realmente Ray ante el crash, la lentitud y la
concurrencia, y en qué difirió de lo esperado)_
