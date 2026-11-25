# Comparación Ray / nativo

> ⬜ **Esqueleto.** No se exige que la versión nativa supere a Ray. Una conclusión fuerte puede
> ser que la versión nativa es más pequeña y comprensible **precisamente porque omite**
> scheduling general, object management, autoscaling y otras capacidades que el problema no
> necesita.

## Mismo workload, mismas condiciones

_(máquina, datos, semilla, parámetros comunes; F1 y F2 con el mismo SDK y los mismos tests de
contrato)_

## Dimensiones (metodología)

| Dimensión | Fase 1 (Ray) | Fase 2 (nativo) | Cómo se midió | Comentario |
|---|---|---|---|---|
| Complejidad de implementación | | | (LOC por módulo, nº de mecanismos) | |
| Mecanismos explícitos | | | | |
| Dependencias | | | | |
| Startup | | | (tiempo hasta servir la primera búsqueda) | |
| Throughput | | | (QPS en W2/W4) | |
| p50 / p95 / p99 | | | | |
| Tiempo de recuperación | | | (crash + restart) | |
| Comportamiento ante fallos | | | | |
| Recursos | | | (CPU, RSS, disco) | |
| Complejidad operacional / debugging | | | | |
| Garantías presentes en una y ausentes en otra | | | | |

## Conclusión

_(pendiente)_
