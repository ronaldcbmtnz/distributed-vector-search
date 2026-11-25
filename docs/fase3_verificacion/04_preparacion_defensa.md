# Preparación de la defensa individual

> ⬜ **Guía de estudio.** **Cada integrante** debe poder operar el sistema y razonar sobre él
> solo. Pasar las pruebas públicas no sustituye esta competencia.

## Lo que la defensa puede pedir (metodología) y dónde prepararlo

| Puede pedir | Cómo prepararlo | Material |
|---|---|---|
| Predecir un fallo nuevo antes de inyectarlo | Practicar con escenarios no ensayados: escribir la predicción y luego ejecutar | campaña F3 |
| Localizar dónde se implementa una garantía | Mapa garantía → archivo:línea | `02_analisis_garantias.md` |
| Identificar un invariante | Lista de invariantes y su test | ídem |
| Interpretar una traza o historia | Leer logs JSON reales ordenados por term/index/query_id | informe §4.3 |
| Explicar por qué una operación es o no idempotente | upsert sin `request_id` (contraejemplo v1/v2), insert/update condicionales | intro D07 |
| Formular consistencia mediante historias observables | Historias permitidas y prohibidas por nivel | `02_analisis_garantias.md` |
| Identificar un punto único de fallo | Recorrer componentes y configuración | informe §5 |
| Justificar timeout / lease | Valores elegidos, medidos frente a p95/p99 y riesgo de elecciones espurias | intro D09, D11 |
| Modificar una parte pequeña | Ensayo: cambiar k máximo, añadir un operador de filtro, cambiar el plazo por defecto… | código |
| Comparar una arquitectura alternativa | A frente a B frente a C de D05; leaderless; gossip | intro D05 |
| Explicar una sugerencia de IA aceptada o rechazada | Lista de casos concretos (abajo) | este archivo |

## Sugerencias de IA aceptadas / rechazadas (registro)

| Fecha | Sugerencia | ¿Aceptada? | Por qué / cómo se verificó |
|---|---|---|---|
| 2026-11-23 | Usar RF = 5 en lugar del RF = 3 de la guía de diseño para cumplir la tolerancia a 2 fallos | ✅ | Argumento de quórum 2f + 1; se verificará en la campaña P1 |
| 2026-11-23 | Raft por shard en vez de plano de control + primario-backup (guía) | ✅ | Un solo protocolo; mismo coste por escritura (1 RTT a la mayoría); se verificará con tests de invariantes |

## Ensayos

| Fecha | Integrante | Escenario sorpresa | Predicción | Resultado | Notas |
|---|---|---|---|---|---|
