# 3. Fichas por componente

> ⬜ **Esqueleto.** Se repite la misma ficha por cada componente.
> Componentes previstos (Raft por shard, RF = 5):
> 1. Cliente / SDK de referencia
> 2. Gateway (API + coordinador de consultas)
> 3. Proceso de almacenamiento (nodo)
> 4. Réplica de shard: módulo de consenso (Raft)
> 5. Réplica de shard: máquina de estados (`ShardCore` + dedup)
> 6. Almacenamiento persistente (log + estado de Raft + snapshots)
> 7. Configuración de clúster (bootstrap)
>
> Herramientas de verificación (fuera del runtime, pero se describen): inyector de fallos y
> checker de historias.

---

## Plantilla de ficha

### Componente: _nombre_

| Campo | Contenido |
|---|---|
| **Responsabilidad** | |
| **Ubicación** | (qué nodo/proceso; cuántas instancias) |
| **Entradas** | (mensajes que recibe, de quién) |
| **Salidas** | (mensajes que emite, a quién) |
| **Estado que posee** | (qué, dónde vive, si es durable, quién es autoritativo) |
| **Dependencias** | |
| **Protocolo** | (RPC, formato, versión, timeouts) |

**Cómo toma decisiones:** _(p. ej. qué réplica elegir, cuándo hacer hedge, cuándo confirmar,
cuándo convocar elección)_

**Por qué este diseño:** _(alternativas consideradas y compromiso)_

**Fallo propio:** _(qué pasa si este componente cae, se cuelga o se reinicia; quién lo detecta;
qué estado se pierde o se recupera)_

**Fallo de una dependencia:** _(qué hace si su dependencia está lenta, caída o particionada)_

**Evidencia:** _(tests y experimentos que lo respaldan)_

---

## 3.1 Cliente / SDK

_(pendiente)_

## 3.2 Gateway

_(pendiente)_

## 3.3 Proceso de almacenamiento

_(pendiente)_

## 3.4 Consenso por shard

_(pendiente)_

## 3.5 Máquina de estados del shard

_(pendiente)_

## 3.6 Almacenamiento persistente

_(pendiente)_

## 3.7 Configuración de clúster

_(pendiente)_
