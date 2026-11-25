# 2. Arquitectura sin Ray

> ⬜ **Esqueleto.** Fuente: `../00_introduccion.md` §7–8.

## 2.1 Nodos, procesos y canales

_(diagrama: 5 nodos; en cada uno, los procesos que aloja; canales HTTP externos y gRPC internos,
con el tipo de mensajes de cada canal)_

| Nodo | Procesos que aloja | Puertos | Volúmenes |
|---|---|---|---|
| nodo-1..5 | gateway, storage (réplicas de los shards s0..sS−1) | | |

| Canal | Extremos | Protocolo | Mensajes | Deadline / timeout |
|---|---|---|---|---|
| Cliente → gateway | | HTTP/JSON | | |
| Gateway → storage | | gRPC | Search, Write, Get, Status | |
| Storage ↔ storage | | gRPC | RequestVote, AppendEntries | |

## 2.2 Qué hacía Ray y quién lo hace ahora

| Responsabilidad que asumía Ray | Quién la asume ahora | Cómo (mecanismo y dónde en el código) |
|---|---|---|
| Ejecución remota | | |
| Directorio de actores (GCS) | | |
| Placement / scheduling | | |
| Esperas con timeout (`ray.wait`) y cancelación | | |
| Reinicio de actores (`max_restarts`) | | |
| Serialización | | |
| Serialización de llamadas al estado (buzón del actor) | | |
| _(otras que salgan del mapa de dependencias)_ | | |

## 2.3 Capacidades de Ray que deliberadamente no reconstruimos

_(scheduler general, object store, autoscaling, lineage… y por qué este problema no las necesita)_

## 2.4 Despliegue

_(Docker Compose, configuración, cómo se mapean nodos y procesos a contenedores, volúmenes,
parámetros configurables)_
