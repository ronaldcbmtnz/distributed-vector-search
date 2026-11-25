# Fase 3 — Verificación, comparación y defensa

> **Peso:** 35 %:
> - Campaña de fallos/carga: 10 %
> - Análisis de garantías: 5 %
> - Comparación Ray/nativo: 5 %
> - Defensa individual: 10 %
> - Reproducibilidad, informe y disclosure: 5 %
>
> **La pregunta cambia** de *"¿funciona el caso normal?"* a *"¿qué garantiza el sistema bajo los
> fallos y cargas relevantes, y qué evidencia respalda esa afirmación?"*. Esta fase **no** es
> para añadir features.

| Documento | Contenido | Estado |
|---|---|---|
| [`01_campana_fallos_carga.md`](01_campana_fallos_carga.md) | Campaña mínima común + específica, con registro experimental | ⬜ |
| [`02_analisis_garantias.md`](02_analisis_garantias.md) | Qué se garantiza, bajo qué condiciones y con qué evidencia | ⬜ |
| [`03_comparacion_ray_nativo.md`](03_comparacion_ray_nativo.md) | Comparación en las dimensiones de la metodología | ⬜ |
| [`04_preparacion_defensa.md`](04_preparacion_defensa.md) | Guía de estudio y ensayo para la defensa individual | ⬜ |

## Checklist

- [ ] Cada experimento tiene los 10 campos del registro y evidencia legible por máquina.
- [ ] Los IDs de escenario siguen el vocabulario del curso (F0–F21) cuando aplican.
- [ ] Los escenarios no aplicables están justificados (no se inventan componentes para poder
  ejecutarlos).
- [ ] Todo se reproduce con `make exp SCENARIO=<id>` desde cero.
- [ ] `ai_use.yaml` está actualizado.
