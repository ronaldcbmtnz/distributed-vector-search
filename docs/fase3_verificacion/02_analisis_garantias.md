# Análisis de garantías

> ⬜ **Esqueleto.** Responde a la pregunta del enunciado: *¿qué afirmaciones son garantías de
> safety/corrección y cuáles son objetivos de disponibilidad o rendimiento?* y *¿qué evidencia
> concreta las demuestra, limita o falsa?*

## Tabla de afirmaciones

| # | Afirmación | Tipo | Condiciones | Mecanismo que la sostiene (código) | Evidencia (test / experimento) | Resultado | Límites observados |
|---|---|---|---|---|---|---|---|
| A1 | Una escritura confirmada no se pierde ante ≤ 2 fallos | Safety | crash-recovery, disco persistente | | | | |
| A2 | No hay split-brain por shard | Safety | cualquiera | | | | |
| A3 | Reintento idempotente | Safety | dentro de la ventana de retención | | | | |
| A4 | Prefijo consistente por shard (`eventual`) | Safety | | | | | |
| A5 | RYW / `strong` según el contrato | Safety | | | | | |
| A6 | Parcial siempre marcado | Safety | | | | | |
| A7 | Top-k exacto con cobertura completa | Safety | índice exacto | | | | |
| A8 | Búsquedas con cobertura completa con ≤ 2 fallos | Liveness | tras la ventana de detección | | | | |
| A9 | Escrituras aceptadas tras `T_FAILOVER` | Liveness | mayoría conectada | | | | |
| A10 | p99 de búsqueda ≤ objetivo | Rendimiento | W2, sin fallos | | | | |

## Formulación de consistencia mediante historias observables

_(para cada nivel: ejemplos de historias **permitidas** y **prohibidas**, y cómo las detecta el
checker)_

## Invariantes del sistema

_(lista de invariantes comprobados en los tests: Election Safety, Log Matching, Leader
Completeness, State Machine Safety, `applied ≤ commit`, ningún id con vector y metadatos de
versiones distintas…)_

## Puntos únicos de fallo y supuestos residuales

_(¿queda alguno? configuración estática, máquina anfitriona única, disco compartido…)_
