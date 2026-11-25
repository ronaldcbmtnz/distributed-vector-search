# Campaña de fallos y carga

> ⬜ **Esqueleto con la lista de escenarios prevista.** Los escenarios se derivan del diseño y de
> las garantías declaradas, no al revés.

## Workloads reutilizables

| ID | Workload | Descripción |
|---|---|---|
| W1 | Sanity secuencial | upsert → get → search → delete → search |
| W2 | Concurrentes independientes | N clientes, ids disjuntos, 80 % búsquedas / 20 % escrituras |
| W3 | Hot key / hot shard | Escrituras y búsquedas concentradas en pocos ids de un mismo shard |
| W4 | Read-heavy / write-heavy | 95/5 y 30/70 |
| W5 | Fan-out | Búsquedas con k alto y filtros poco selectivos (todas las consultas tocan todos los shards) |
| W6 | Carga masiva | Lote de 100 000 vectores (mide el throughput de escritura) |

## Campaña mínima común (metodología)

| # | Requisito de la metodología | Escenario(s) | Afirmación bajo prueba | Estado |
|---|---|---|---|---|
| 1 | Comportamiento normal | **F0** baseline con W2/W4 | Latencias y throughput de referencia; recall = 1 | ⬜ |
| 2 | Crash de un nodo de servicio | **F1** (follower), **F15** (líder de shards) | Búsquedas con cobertura completa; escrituras tras la elección | ⬜ |
| 3 | Crash/restart y recuperación | **F2** | La réplica vuelve, se pone al día y queda "lista"; se mide el tiempo | ⬜ |
| 4 | Degradación por retraso o pérdida | **F3** (netem delay), **F4** (loss) | El hedge contiene el p99; detector sin elecciones espurias excesivas | ⬜ |
| 5 | Partición y heal/reconciliación | **F5** (partición 2\|3), **F7** (heal) | La minoría no confirma; tras el heal se truncan las entradas no confirmadas | ⬜ |
| 6 | Duplicado / respuesta perdida | **F8**, **F9** | Mismo `request_id` → mismo resultado, sin duplicados ni pisadas | ⬜ |
| 7 | Workload concurrente | W2 + W3 + checker | Ninguna mezcla vector/metadatos; prefijos consistentes | ⬜ |
| 8 | Sobrecarga sostenida | **F16**, **F17** | Rechazo `overloaded` rápido; sin colapso ni amplificación | ⬜ |

## Otros escenarios del vocabulario del curso

| ID | Aplica | Escenario / justificación |
|---|---|---|
| F6 | Sí | Partición simétrica de aplicación (gateways que ven réplicas distintas) |
| F10 | Sí | Descubrimiento obsoleto: gateway con caché de líder vieja → `not_leader` + pista |
| F11 | Sí | Lectura de réplica atrasada: `eventual` frente a `read_your_writes` frente a `strong` |
| F12 | Sí | Escrituras concurrentes conflictivas sobre el mismo id: el orden del líder decide |
| F13/F14 | **No** | No hay 2PC: ninguna operación toca más de un shard (D01) |
| F18 | **No** (salvo extensión) | Membresía estática; no hay churn por diseño (D11) |
| F19 | **No** | No hay escrituras desconectadas: la minoría no escribe (no hay CRDT) |
| F20 | Sí | Operación no autorizada: API key inválida o colección sin permiso |
| F21 | Sí | Replay de una mutación autenticada → dedup |

## Campaña específica del proyecto (P19)

| ID | Escenario | Afirmación bajo prueba |
|---|---|---|
| P1 | **Dos fallos simultáneos**, por clases de pares (2 nodos; líder + follower del mismo shard; gateway + nodo; 2 gateways) | Funcionalidad completa tras `T_FAILOVER`; ninguna escritura confirmada perdida |
| P2 | **Shard no disponible** (> 2 fallos o plazo corto) y política de parciales | Respuesta dentro del plazo, marcada; `allow_partial=false` → 503 |
| P3 | **recall@k frente a cobertura** en modo degradado | recall ≈ fracción del top-k verdadero alojada en shards presentes (métrica diferenciadora) |
| P4 | **Líder zombi:** el líder viejo aislado reaparece | Sus escrituras no se confirman; se truncan al reconectarse |
| P5 | **Upsert seguido de búsqueda inmediata** sin token / con token / `strong` | La ventana de lectura obsoleta existe con `eventual` y desaparece con RYW o `strong` |
| P6 | **`kill -9` durante una carga masiva** | Qué estado queda, quién lo recupera, nada confirmado se pierde |
| P7 | **Reinicio de una réplica en mitad de una votación** | No vota dos veces en el mismo término (votos persistidos) |
| P8 | **Detector bajo retraso** | Latencia de detección y número de falsas sospechas o elecciones espurias |

## Plantilla de registro experimental (por experimento)

```
### <ID> — <título>
- Escenario:
- Workload:
- Afirmación bajo prueba:
- Predicción:
- Configuración: (nodos, S, RF, plazos, nº de vectores, d, semilla, commit)
- Trigger del fallo: (tipo, objetivo, inicio, fin)
- Evidencia recogida: (rutas a logs/CSV/historias)
- Resultado observado:
- Conclusión:
- Incertidumbre restante:
```

## Números a reportar siempre

- QPS.
- Nº de vectores y dimensión, S y RF.
- p50/p95/p99 global y por shard.
- Fracción de parciales y recall@k.
- Retraso de replicación.
- Tiempo de failover.
- Tiempo de recuperación.
