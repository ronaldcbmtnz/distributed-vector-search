# 1. Servicio y garantías

> ⬜ **Esqueleto.** Fuente: `../00_introduccion.md` §2 y §4 + `../fase1_ray/01_contrato_api.md`.

## 1.1 Clientes

_(quiénes son, cómo acceden, cuántos concurrentes se soportan)_

## 1.2 Operaciones que necesitan

_(tabla de operaciones con su efecto observable; referencia al contrato congelado)_

## 1.3 Resultados observables prometidos

Formular cada garantía como **historias observables**:

- **Durabilidad:** _"Si el cliente recibió 2xx para una escritura, toda búsqueda `strong`
  posterior la refleja, aunque entre medias caigan dos componentes cualesquiera."_
- **Idempotencia:** _…_
- **Visibilidad (3 niveles):** _…_ + declaración explícita de que no hay snapshot global.
- **Resultados degradados:** _…_ (formato `partial` / `coverage` / `missing_shards`).
- **Disponibilidad por operación:** _"Con ≤ 2 componentes caídos…"; "En una partición, el lado
  con mayoría de un shard…"_
- **Concurrencia:** _…_

## 1.4 Lo que el sistema NO garantiza

_(snapshot global, más de 2 fallos, pérdida de disco, bizantinos, atacante interno)_

## 1.5 Modelo de fallos y de confianza

_(crash-recovery con disco estable; omisión, retraso y partición; relojes no sincronizados;
contenedores en una máquina simulan dominios de fallo; red interna confiable)_

## 1.6 Safety frente a liveness

| Afirmación | Tipo | Condición | Evidencia |
|---|---|---|---|
| | safety / liveness / objetivo de rendimiento | | |
