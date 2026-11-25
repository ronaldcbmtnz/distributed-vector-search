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

## Semántica de Ray observada (sonda T110a)

Medido con Ray 2.60.1 en una sola máquina. Reproducible con `make test-integration`
(`tests/integration/test_ray_semantics.py`).

| Pregunta | Observado | Consecuencia en el diseño |
|---|---|---|
| ¿Respeta Ray el orden de las llamadas de un mismo llamador a un actor **async**? | **No.** En 2000 llamadas aparecen inversiones de orden, incluso en métodos sin `await` | Los backups guardan las entradas en un buffer por `seq` y solo aplican de forma contigua (`BackupLog`). Sin ese buffer, el orden total por shard se rompería. |
| ¿Qué estado queda tras reiniciar un actor (`max_restarts > 0`)? | El actor vuelve con `was_current_actor_reconstructed = True` y el **estado vacío**: 0,12 s tras `ray.kill`, 1,76 s tras `kill -9` del proceso | Ray da disponibilidad del *proceso*, no de los *datos*. La réplica reconstruida se marca **no lista** y no sirve lecturas. En F2 harán falta persistencia y puesta al día. |
| ¿Qué ve el llamador mientras el actor se reinicia? | `ActorUnavailableError` durante el hueco | El transporte lo trata como `ReplicaUnreachable` → failover a otra réplica |
| ¿Y sin reinicios (`max_restarts = 0`)? | `ActorDiedError` (subclase de `RayActorError`) al instante | Ídem: la réplica queda fuera y el coordinador usa las demás |
| ¿Un timeout local (`asyncio.wait_for`) detiene el trabajo remoto? | **No.** El actor sigue ejecutando la llamada | El transporte llama a `ray.cancel` cuando vence el timeout o se cancela la espera |
| ¿Funciona `ray.cancel` sobre una tarea async en curso? | Sí: el actor recibe `CancelledError` y el llamador `TaskCancelledError` | Los hedges perdedores y lo pendiente al vencer el plazo se cancelan de verdad |
| ¿Hay backpressure por handle (`max_pending_calls`)? | Sí: `PendingCallsLimitExceeded` al **enviar** la llamada, en el lado del llamador | Se mapea a `ReplicaOverloaded` → `overloaded` (429) en escrituras, otra réplica en lecturas |
| ¿Es idempotente crear un actor con nombre y `get_if_exists=True`? | Sí: devuelve el mismo actor con su estado | El `Catalog` puede reejecutar el arranque sin duplicar réplicas |

## Observaciones del experimento

_(completar con lo observado en T121b: qué hizo realmente Ray ante el crash, la lentitud y la
concurrencia en el despliegue con varios nodos, y en qué difirió de lo esperado)_
