# Fase 1 — Prototipo distribuido con Ray

> **Entrega:** 1 dic 2026, 23:59 UTC−4 · **Peso:** 15 %
> (prototipo 7 % · arquitectura + dependencias 5 % · experimento inicial 3 %).
> La tolerancia a fallos **no** se exige en esta parte.

## Propósito (según la metodología)

Ray Core funciona temporalmente como andamiaje de middleware para entender el problema antes de
invertir en infraestructura. La pregunta no es solo *"¿puede Ray resolver esto?"*, sino también
*"¿qué tuvo que hacer Ray para que este código, aparentemente simple, funcionara entre procesos
o máquinas?"*.

## Artefactos requeridos

| # | Artefacto | Documento / ubicación | Estado |
|---|---|---|---|
| 1 | Prototipo Ray funcional con ejecución distribuida real (varios workers/procesos) | código en `src/`; despliegue en `deploy/` | ⬜ |
| 2 | Diagrama de arquitectura: componentes lógicos + runtime Ray del que dependen | [`02_arquitectura_ray.md`](02_arquitectura_ray.md) | ⬜ |
| 3 | Contrato de comportamiento/API **sin** detalles de Ray | [`01_contrato_api.md`](01_contrato_api.md) | 🚧 |
| 4 | Mapa de dependencias del middleware | [`03_mapa_dependencias_middleware.md`](03_mapa_dependencias_middleware.md) | 🚧 |
| 5 | Experimento inicial (fallo, concurrencia o workload controlado) | [`04_experimento_inicial.md`](04_experimento_inicial.md) | ⬜ |

## Checklist de entrega

- [ ] `make up-ray` levanta el clúster (head + workers) y la API.
- [ ] `make test` pasa los tests de contrato.
- [ ] Los logs muestran actores en **procesos y nodos distintos**.
- [ ] El experimento inicial tiene evidencia legible por máquina (logs/CSV), no solo capturas.
- [ ] Están `ai_use.yaml` y el aviso de terceros en el README.
- [ ] Tag `v1-ray`.
- [ ] Issue en GitHub con tema, nombres completos + grupos y enlace.

## Puerta de salida (responder antes de cruzar la frontera de Ray)

> Tras responderlas, el contrato observable **se congela**. Solo se cambia con justificación
> explícita en el registro de decisiones.

1. **¿Qué problema resuelve la aplicación?**
   _(pendiente — resumen de `00_introduccion.md` §1–2)_
2. **¿Qué comportamiento externo debe sobrevivir al rediseño?**
   _(pendiente — referencia a `01_contrato_api.md`)_
3. **¿Qué comportamiento viene de mi código?**
   _(pendiente — p. ej. merge determinista, parciales marcados, dedup, filtros, validación, niveles de consistencia)_
4. **¿Qué comportamiento viene de Ray?**
   _(pendiente — p. ej. ejecución remota, directorio de actores, placement, serialización, reinicio de actores, serialización de llamadas en el actor)_
5. **¿Qué semántica de fallo observé en vez de asumir?**
   _(pendiente — resultados del experimento inicial)_
