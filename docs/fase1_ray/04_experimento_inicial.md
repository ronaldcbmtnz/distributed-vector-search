# Experimento inicial (Fase 1)

> ⬜ **Esqueleto.** Peso: 3 %. **Qué pide la metodología:** al menos un fallo, concurrencia o
> workload controlado que revele el comportamiento del runtime **antes** de retirarlo.
> Evidencia preferente: logs, métricas o CSV legibles por máquina. Las capturas son solo apoyo.

## Plantilla de registro (los 10 campos de la metodología)

| Campo | Contenido |
|---|---|
| Escenario | |
| Workload | |
| Afirmación bajo prueba | |
| Predicción | |
| Configuración | (S, RF, nodos Ray, `max_restarts`, plazos, nº de vectores, d, semilla) |
| Trigger del fallo | (qué, cuándo, cómo) |
| Evidencia recogida | (ficheros en `experiments/results/...`) |
| Resultado observado | |
| Conclusión | |
| Incertidumbre restante | |

---

## E1 — Crash de un actor de shard durante una carga mixta

- **Afirmación bajo prueba:** "Ray recupera el actor, pero no su estado".
- **Predicción:**
  - Con `max_restarts = 0`, las búsquedas reciben `RayActorError` para ese shard y el sistema
    responde parcial marcado.
  - Con `max_restarts > 0`, el actor vuelve con el **índice vacío** y las búsquedas posteriores
    devuelven resultados sin los vectores de ese shard **sin marcarlo como parcial**. Es el
    hallazgo clave para el rediseño: hace falta persistencia y un estado "lista".
- **Trigger:** `kill -9` del proceso worker que aloja el actor, y `ray.kill(actor)`.
- _(registro pendiente)_

## E2 — Shard lento y política de parciales

- **Afirmación bajo prueba:** "Con plazo global, una búsqueda nunca espera más de `deadline_ms`
  + margen, y marca el shard faltante".
- **Predicción:** p99 ≈ `deadline_ms`; fracción de parciales ≈ fracción de consultas afectadas;
  recall@k ≈ cobertura.
- **Trigger:** retraso artificial configurable en un actor de shard.
- _(registro pendiente)_

## E3 — Escrituras y búsquedas concurrentes sobre los mismos ids

- **Afirmación bajo prueba:** "El buzón del actor serializa las operaciones: una búsqueda nunca
  ve vector y metadatos de versiones distintas".
- **Predicción:** 0 mezclas detectadas por el checker. Latencia de búsqueda creciente con la
  tasa de escrituras en ese shard.
- _(registro pendiente)_

## Conclusiones para el rediseño

_(completar: qué semántica de fallo **observamos** en lugar de asumir, y cómo cambia el mapa de
dependencias)_
