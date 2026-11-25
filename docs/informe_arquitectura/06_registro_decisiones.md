# Registro de decisiones

> 🚧 **Prerrelleno.** **Qué pide la metodología:** una tabla compacta que demuestre que las
> abstracciones de Ray se retiraron **deliberadamente**, y no que se sustituyó Ray por una
> colección aleatoria de tecnologías.
> Detalle y justificación completa de cada fila en `../00_introduccion.md` §5.

| Requisito | Fase 1 (Ray) | Alternativas nativas | Diseño elegido | Razón / compromiso |
|---|---|---|---|---|
| Discovery | Runtime (GCS, actores con nombre) | Estático / registro / gossip / DHT | Estático + pista de líder (D11) | 5 nodos: lookup de un salto; lo dinámico (líder) lo da el consenso |
| Colecciones | Actor `Catalog` (cargado de config) | Config estática / catálogo dinámico replicado | Config estática (C3) | No hace falta un grupo de consenso de metadatos; el contrato ya admite añadirlo |
| Estado del shard | Actor primario + backups en memoria | Disco local / primario-backup / log replicado / CRDT | Log replicado (Raft) por shard, RF = 5 (D05) | Tolera 2 fallos; un solo algoritmo para elección, fencing y puesta al día. Coste: 5 copias |
| Reintento | Runtime (`max_restarts`, `max_task_retries`) | Idempotencia / dedup / sin reintento | `request_id` + dedup en el estado replicado (D07) | Reintentos seguros incluso tras failover |
| Failover | Runtime (reinicio con estado vacío) | Restart / elección / reroute | Elección Raft + reroute en el gateway + restart del contenedor | Sin componente externo que decida; término como fencing |
| Placement | Scheduler de Ray | Dinámico / estático | Estático en configuración | El problema no necesita scheduling dinámico |
| Ordenación de escrituras | Buzón del actor (por llamador) | Relojes / secuencia del líder | Índice del log por shard (D06) | Un escritor por shard: orden total sin relojes |
| Visibilidad | (implícita) | Eventual / RYW / lineal | 3 niveles elegibles (D08) | Honestidad sobre réplicas atrasadas; coste solo si se pide |
| Degradación | `ray.wait(timeout)` | Esperar / fallar / parcial | Plazo global + hedge + parcial marcado (D09) | Lecturas: disponibilidad. Escrituras: consistencia |
| Comunicación | Llamadas a actores | REST / gRPC / broker | REST externo, gRPC interno, sin broker (D14) | Contrato tipado y deadlines; un broker sustituiría el mecanismo |
| Backpressure | `max_concurrency` (implícito) | Colas sin límite / admisión | Semáforos + colas acotadas + 429 (D13) | Fallar rápido; evitar amplificación |
| Durabilidad | Ninguna | Sin persistencia / WAL / WAL + snapshot | Log Raft con `fsync` + snapshot (D10) | Confirmar solo lo persistido en mayoría |
| Seguridad | Ninguna | — | API key + permisos por colección + límites (D16) | Red interna confiable (declarado) |
| Observabilidad | Dashboard de Ray | — | Logs JSON con IDs + métricas + historias (D17) | Evidencia legible por máquina para F3 |
| Lenguaje | Python | Python / Go | Python (D18) | Dominio numérico; reutilización; comparación justa |
