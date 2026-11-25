# Contrato de comportamiento y API

> 🚧 **Borrador.** Se congela al cerrar la Fase 1 (tarea T125). Este contrato es **idéntico**
> en la versión con Ray y en la nativa. No menciona actores, ObjectRefs ni nada del runtime: el
> cliente depende de *"almacenar vector"* y *"buscar"*, no de *"llamar al actor X"*.
>
> Las colecciones se definen por configuración de despliegue: la API no las crea ni las borra,
> pero todas las rutas ya llevan `/collections/{c}`.
>
> Cada colección usa una métrica fija:
> - **L2:** `distance` es la distancia euclídea.
> - **Coseno:** `distance` = 1 − cos. En las colecciones coseno se rechaza el vector nulo.
>
> La deduplicación por `request_id` no expira.

## 1. Conceptos

| Concepto | Definición observable |
|---|---|
| Colección | Conjunto de vectores con dimensión `dim` y métrica fijas. Es la unidad de permisos. |
| Vector | `id` (string elegido por el cliente, ≤ 128 bytes) + `vector` (`dim` floats finitos) + `metadata` (objeto JSON plano). |
| Metadatos | Claves string → valor `string \| number \| bool` o lista de esos tipos. Límite de tamaño por definir. |
| Shard | Subconjunto fijo de la colección, determinado solo por el `id`. El cliente lo ve en tokens y en la información de cobertura. |
| Token de escritura | `{shard, seq}`: posición de la escritura en la historia de su shard. Se usa para `read_your_writes`. |
| `request_id` | Identificador único de una escritura lógica, generado por el cliente (UUID). Obligatorio. |

## 2. Autenticación

Toda petición lleva la cabecera `Authorization: Bearer <api_key>`. Cada API key tiene permisos
`read` y/o `write` por colección.

## 3. Operaciones

### 3.1 Upsert — crear o reemplazar

```
PUT /v1/collections/{c}/vectors/{id}
{ "vector": [..], "metadata": {..}, "request_id": "uuid" }
→ 200 { "id": "..", "token": {"shard": 3, "seq": 1042}, "created": true|false }
```

### 3.2 Insert condicional — falla si existe

```
POST /v1/collections/{c}/vectors
{ "id": "..", "vector": [..], "metadata": {..}, "request_id": "uuid" }
→ 201 { "id": "..", "token": {..} }   |  409 already_exists
```

### 3.3 Update condicional — falla si no existe

```
PATCH /v1/collections/{c}/vectors/{id}
{ "vector"?: [..], "metadata"?: {..}, "request_id": "uuid" }
→ 200 { "id": "..", "token": {..} }   |  404 not_found
```

Si se envía `metadata`, reemplaza el objeto **completo** (no se fusiona por claves). Si no se
envía, se conservan los metadatos actuales. Lo mismo vale para `vector`.

### 3.4 Delete

```
DELETE /v1/collections/{c}/vectors/{id}?request_id=uuid
→ 200 { "id": "..", "token": {..}, "existed": true|false }
```

### 3.5 Get

```
GET /v1/collections/{c}/vectors/{id}?consistency=eventual|read_your_writes|strong&min_seq=1042
→ 200 { "id": "..", "vector": [..], "metadata": {..}, "seq": 1042, "applied_seq": 1050 }  |  404 not_found
```

`min_seq` solo aplica con `read_your_writes`: es el `seq` del token que devolvió la escritura
de ese id.

### 3.6 Lote de upserts

```
POST /v1/collections/{c}/vectors:batch
{ "items": [ {"id":"..","vector":[..],"metadata":{..},"request_id":"uuid"}, ... ] }
→ 200 { "results": [ {"id":"..","ok":true,"token":{..}} | {"id":"..","ok":false,"error":{..}} ] }
```

No es atómico entre elementos.

### 3.7 Búsqueda

```
POST /v1/collections/{c}/search
{
  "vector": [..],
  "k": 10,                                   // 1..K_MAX (100)
  "filter": { "lang": "es", "price": {"$gte": 10, "$lte": 100}, "tag": {"$in": ["a","b"]} },
  "consistency": "eventual" | "read_your_writes" | "strong",   // por defecto eventual
  "tokens": [ {"shard": 3, "seq": 1042} ],   // solo para read_your_writes
  "allow_partial": true,                     // por defecto true
  "deadline_ms": 300                         // 50..5000, por defecto 300
}
→ 200
{
  "query_id": "q-7f3a",
  "results": [ {"id": "..", "distance": 0.12, "metadata": {..}} ],
  "partial": false,
  "coverage": 1.0,
  "missing_shards": [],                      // [{ "shard": 3, "reason": "unreachable|unserved|stale" }]
  "consistency": "eventual",
  "shards": {
    "0": {"replica": "s0/r1", "status": "ok", "latency_ms": 18, "applied_seq": 1042},
    "3": {"replica": null, "status": "unreachable", "latency_ms": null, "applied_seq": null}
  }
}
```

`distance` sigue la métrica de la colección: euclídea en L2, `1 − cos` en coseno. Los
resultados van ordenados por `(distance, id)` ascendente.

**Lenguaje de filtros:**

- `{campo: valor}` → igualdad.
- `{campo: {"$in": [..]}}` → pertenencia.
- `{campo: {"$gte"|"$gt"|"$lte"|"$lt": n}}` → rango numérico.
- Varias claves se combinan con AND.
- Un campo-lista cumple la igualdad si **contiene** el valor.

### 3.8 Operación

```
GET /v1/health        → 200 { "status": "ok" }  |  503 mientras el servicio no está listo
GET /v1/metrics       → 200 { latencias p50/p95/p99 por operación y por shard, contadores, lag de replicación }
GET /v1/admin/status  → 200 { estado de cada réplica: rol, ready, applied_seq, commit_seq, nodo }  (requiere clave admin)
```

## 4. Errores

| Código HTTP | `error.code` | Cuándo | ¿Reintentar? |
|---|---|---|---|
| 400 | `invalid_argument` | Dimensión, NaN/Inf, `k`, filtro o límites | **No** |
| 401 | `unauthenticated` | Falta la API key o no es válida | No |
| 403 | `permission_denied` | Sin permiso sobre la colección | No |
| 404 | `not_found` | Colección o id inexistente (update/get) | No |
| 409 | `already_exists` | Insert sobre un id existente | No |
| 409 | `idempotency_conflict` | `request_id` reutilizado con otro contenido | No |
| 429 | `overloaded` | Admisión llena o rate limit | Sí, con backoff |
| 503 | `unavailable` | Shard sin responsable o sin mayoría; búsqueda con `allow_partial=false` y shards faltantes | Sí, con backoff y el **mismo** `request_id` |
| 504 | `deadline_exceeded` | Escritura no confirmada a tiempo: **resultado desconocido** | Sí, con el **mismo** `request_id` |

Formato: `{ "error": { "code": "..", "message": "..", "details": {..} } }`.

## 5. Garantías

| # | Garantía | Aplica en |
|---|---|---|
| G1 | Una escritura confirmada (2xx) no se pierde. En la versión nativa, ni ante dos fallos simultáneos. | F2 (F1 sin tolerancia a fallos) |
| G2 | Reintentar con el mismo `request_id` devuelve el resultado original y nunca reaplica. | F1 y F2 |
| G3 | Vector y metadatos de una escritura se ven siempre juntos. | F1 y F2 |
| G4 | Las escrituras de un mismo shard tienen un orden total. Para un id gana la última en ese orden. | F1 y F2 |
| G5 | `eventual`: cada shard responde desde un prefijo de su historia confirmada. | F1 y F2 |
| G6 | `read_your_writes`: cada shard con token responde desde un estado que incluye ese token, o figura como faltante (`stale`). | F1 y F2 |
| G7 | `strong`: cada shard incluye todo lo confirmado antes del inicio de la búsqueda. | F1 y F2 |
| G8 | La respuesta llega dentro de `deadline_ms` + margen; si falta algún shard, va marcada. | F1 y F2 |
| G9 | Con cobertura completa, el top-k es exacto y determinista (desempate por id). | F1 y F2 |
| G10 | **No** hay snapshot global entre shards. | (no garantía, declarada) |

## 6. Historial de cambios del contrato

| Fecha | Cambio | Justificación |
|---|---|---|
| 2026-11-23 | Borrador inicial | — |
