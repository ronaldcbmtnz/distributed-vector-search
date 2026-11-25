# 4. Operación de extremo a extremo

> ⬜ **Esqueleto.** Se sigue **una petición propia del proyecto** desde el cliente hasta el
> resultado, identificando en cada paso los **mensajes**, las **decisiones** y los **cambios de
> estado**.
> Propuesta: dos operaciones encadenadas que ejercitan lo distintivo del sistema:
> 1. `upsert` de un vector → token `{shard, seq}`.
> 2. `search` con `consistency = read_your_writes` y ese token, con filtro.

## 4.1 Escritura: upsert

| Paso | Dónde | Mensaje (de → a) | Decisión tomada | Cambio de estado | Log emitido |
|---|---|---|---|---|---|
| 1 | Cliente | genera `request_id`; HTTP PUT → gateway | | | |
| 2 | Gateway | valida, autentica, admite; calcula el shard | | | |
| 3 | Gateway → líder | Write(...) | ¿quién es el líder? (caché / `not_leader`) | | |
| 4 | Líder | ¿dedup? append al log + `fsync` | | | |
| 5 | Líder → followers | AppendEntries | | | |
| 6 | Followers | comprueban término y `prevLog`; append + `fsync`; ack | | | |
| 7 | Líder | mayoría → `commitIndex`; aplica a la máquina de estados | | | |
| 8 | Líder → gateway → cliente | respuesta con token | | | |

## 4.2 Búsqueda RYW con filtro

| Paso | Dónde | Mensaje | Decisión | Cambio de estado | Log |
|---|---|---|---|---|---|
| 1 | Gateway | `query_id`, plazo, una réplica por shard | | | |
| 2 | Réplicas | ¿`applied ≥ token`? si no, `stale` | | | |
| 3 | Gateway | hedge / redirección al líder del shard atrasado | | | |
| 4 | Réplicas | pre-filtrado + top-k local | | | |
| 5 | Gateway | merge `(dist, id)`; cobertura | | | |
| 6 | Cliente | recibe resultados + metadatos de calidad | | | |

## 4.3 Traza real

_(fragmento de logs JSON de una ejecución real, ordenado por `term`/`index` y `query_id`)_
