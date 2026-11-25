# Arquitectura de la Fase 1 (sobre Ray)

> ⬜ **Esqueleto.** Se completa al terminar la implementación (T123).
> **Qué pide la metodología:** un diagrama con los **componentes lógicos de la aplicación** y
> los **componentes relevantes del runtime Ray** de los que dependen.

## 1. Vista general (borrador)

```
  Cliente (SDK/CLI, curl)
        │ HTTP/JSON
        ▼
  ┌──────────────────────────── proceso gateway (driver de Ray) ───────────────────────────┐
  │  API REST (validación, auth, admisión) → Coordinador (scatter-gather, merge, plazos)    │
  └───────────────┬───────────────────────────────────────────────┬────────────────────────┘
                  │ .remote() / ray.wait                          │ .remote()
                  ▼                                               ▼
       ┌─────────────────────┐                     ┌───────────────────────────────┐
       │ Actor Catalog       │                     │ Actores ShardReplica (S × RF)  │
       │ colecciones, mapa   │                     │ ShardCore: índice + metadatos  │
       │ de shards (versión) │                     │ + tombstones + dedup           │
       └─────────────────────┘                     └───────────────────────────────┘
  ─────────────────────────────────── runtime Ray ─────────────────────────────────────────
   GCS (directorio de actores con nombre, metadatos del clúster) · Raylets (scheduling local,
   workers) · Object store (plasma) · Core worker (serialización, envío de tareas, reintentos)
   Nodos: head + workers en contenedores
```

## 2. Componentes lógicos

| Componente | Responsabilidad | Estado | Implementado como |
|---|---|---|---|
| API REST | Validar, autenticar, autorizar y admitir; traducir errores | Ninguno | FastAPI en el proceso gateway |
| Coordinador | Scatter-gather, plazos, hedge, merge, parciales | Ninguno (solo caché del mapa) | Código Python en el gateway |
| Catalog | Colecciones (cargadas del fichero de configuración), dimensión, métrica, permisos, mapa de shards (shard → primario + backups) | Sí (en memoria) | Actor con nombre |
| ShardReplica (primario) | Validar, deduplicar, asignar `seq`, aplicar, reenviar a los backups y confirmar con W acks | Sí (en memoria) | Actor con nombre, `max_restarts` configurable |
| ShardReplica (backup) | Aplicar entradas en orden de `seq` (buffer para las adelantadas); servir búsquedas `eventual`; responder `stale` si `applied < token` | Sí (en memoria) | Actor con nombre |
| ShardCore | Lógica pura: índice, filtros, tombstones, dedup | — | Librería (se reutiliza en F2) |

## 3. Componentes de Ray de los que dependemos

_(completar: qué hace cada uno por nosotros y dónde se nota en el código)_

| Componente de Ray | Para qué lo usamos | Dónde se ve |
|---|---|---|
| GCS | Registrar y encontrar actores con nombre | `ray.get_actor(...)` |
| Scheduler / raylet | Colocar actores en nodos (SPREAD) | `.options(scheduling_strategy=...)` |
| Core worker | Serializar argumentos, enviar llamadas, devolver ObjectRefs | `.remote()` |
| Object store | Resultados grandes (si aplica) | `ray.get` |
| Supervisión de actores | Reiniciar actores muertos | `max_restarts` |

## 4. Flujo de una búsqueda (Fase 1)

_(completar con la implementación real)_

## 5. Flujo de una escritura (Fase 1)

_(completar)_

## 6. Despliegue

_(completar: Docker Compose, número de nodos Ray, cómo se reparten los actores y evidencia de
que corren en procesos y nodos distintos)_
