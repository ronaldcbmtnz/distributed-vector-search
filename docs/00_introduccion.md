# Motor distribuido de búsqueda vectorial: el problema y cómo lo resolvemos

> **Qué es este documento.** Es la puerta de entrada al proyecto. Explica qué problema
> resolvemos, qué lo vuelve difícil cuando se distribuye y, para cada pieza del diseño, por
> qué la elegimos frente a las alternativas. El resto de documentos (contrato, informe de
> arquitectura, campaña de fallos) desarrollan lo que aquí se decide.
>
> **Estado de cada decisión:** ✅ decidida · ⏳ propuesta, pendiente de confirmar.
> Documento vivo: si una decisión cambia, se actualiza aquí y en el registro de decisiones
> (`informe_arquitectura/06_registro_decisiones.md`).

---

## 1. El problema

### 1.1 Búsqueda vectorial en un párrafo

Un *embedding* representa un objeto (un texto, una imagen, un producto) como un vector de
números, de forma que objetos parecidos quedan cerca en el espacio. "Buscar" es encontrar los
**k vectores más cercanos** a un vector de consulta según una distancia (por ejemplo, la
euclídea L2). Normalmente la búsqueda se restringe con un filtro sobre los metadatos del objeto
(`idioma = "es"`, `precio ≤ 100`).

Ejemplo: una tienda guarda un vector por producto y, para "productos parecidos a este que
estén en stock", pide los 10 más cercanos con el filtro `stock = true`.

En una sola máquina esto es sobre todo un problema de **indexación numérica**. El curso lo
mantiene simple a propósito: el reto no es el índice, es **distribuirlo**.

### 1.2 Por qué distribuirlo

| Motivo | Qué falla con un solo nodo |
|---|---|
| **Volumen** | La colección (vectores + índice + metadatos) deja de caber en la memoria de un nodo. |
| **Throughput** | Muchas búsquedas concurrentes saturan la CPU de un proceso. |
| **Disponibilidad** | Si el único nodo cae, el servicio desaparece y, sin copias, los datos también. |

### 1.3 Qué se vuelve difícil al distribuir

1. **La trampa central: la ubicación no sigue a la similitud.** Si repartimos los vectores por
   su id, sabemos dónde *vive* cada uno, pero no dónde están *sus vecinos más cercanos*. Una
   escritura toca un shard, pero cada búsqueda tiene que preguntar a **todos** y fusionar
   resultados (*scatter-gather*).
2. **Latencia de cola.** La respuesta tarda lo que tarda el shard más lento. Un solo nodo lento
   arrastra a todas las búsquedas.
3. **Fallos parciales.** Un shard caído deja la respuesta incompleta. Hay que decidir si se
   espera, se responde parcialmente o se falla, y cómo se le dice al cliente.
4. **Escrituras y búsquedas cruzadas.** Una búsqueda que corre mientras se actualiza un vector
   puede ver la versión vieja, la nueva o, si nadie lo impide, una mezcla de vector nuevo y
   metadatos viejos.
5. **Réplicas atrasadas.** Para sobrevivir a fallos se copian los datos. Entonces la copia que
   responde puede no tener todavía la última escritura: hay que definir qué ve el cliente.
6. **Reintentos y duplicados.** Si se pierde la respuesta de una escritura, el cliente
   reintenta. Sin precauciones, el reintento duplica la operación o pisa una escritura posterior.
7. **No se distingue un nodo lento de uno caído.** Por eso cualquier decisión basada en
   timeouts es una **sospecha**, no una certeza (FLP). El diseño debe seguir siendo correcto
   aunque la sospecha sea falsa.

---

## 2. Clientes, operaciones y lo que el sistema promete

**Clientes:**

- Aplicaciones que mantienen una colección de vectores y la consultan, a través de un SDK o
  CLI de referencia o de HTTP directo.
- El operador, que despliega, observa e inyecta fallos.

**Operaciones (contrato completo en `fase1_ray/01_contrato_api.md`):**

| Operación | Efecto observable |
|---|---|
| `upsert` | Crea o reemplaza vector + metadatos. Devuelve un **token de escritura**. |
| `insert` | Como upsert, pero falla con `already_exists` si el id existe. |
| `update` | Modifica vector o metadatos. Falla con `not_found` si no existe. |
| `delete` | El id deja de aparecer. Devuelve token e indica si existía. |
| `get` | Devuelve el vector vigente o `not_found`. |
| `search` | Top-k por distancia con filtro opcional. Declara `partial`, `coverage`, shards faltantes y nivel de consistencia. |
| lote | Varios upserts. Un resultado por elemento, sin atomicidad entre ellos. |

**Promesas observables (resumen; formuladas con precisión en el contrato):**

- Una escritura **confirmada** no se pierde. En Fase 2, ni ante dos fallos simultáneos.
- Reintentar una escritura con el mismo `request_id` es **seguro**: nunca duplica ni pisa.
- Una búsqueda responde **dentro de su plazo**. Si le falta algún shard, **lo dice**.
- Con cobertura completa, el top-k es **exacto y determinista**.
- El cliente **elige qué frescura necesita**: `eventual`, `read_your_writes` o `strong`.
- **No** hay snapshot global entre shards, y se declara.

---

## 3. Restricciones del curso que condicionan el diseño

| Restricción | Consecuencia en el diseño |
|---|---|
| Fase 1 sobre Ray (entrega 1 dic 2026); la tolerancia a fallos no aplica en F1 | F1 resuelve el contrato funcional **con replicación primario → backups en memoria**, para que los tres niveles de visibilidad sean observables desde el principio. Sin persistencia ni failover. Ray aporta ejecución remota, directorio, scheduling y reinicios. Documentamos qué aporta. |
| Después de F1, Ray está prohibido en runtime | Cada responsabilidad que asumía Ray pasa a un mecanismo nuestro, explícito y probado (§7). |
| El contrato observable se congela tras F1 | El contrato se diseña completo (consistencia, tokens, parciales) aunque F1 no lo explote todo. |
| **Defensa: funcionalidad y recuperación ante el fallo simultáneo de 2 componentes cualesquiera** (2 procesos, 2 nodos o proceso + nodo) | Todo quórum de mayoría necesita **2f + 1 = 5** miembros. Ningún componente puede ser único (D05, D15). |
| Ejecución local y reproducible; pruebas públicas deterministas | Docker Compose en una máquina. Red simulada para tests de protocolo. Semillas fijas. |
| "Go es la opción normal"; otro lenguaje se justifica por el dominio | Decisión D18. |
| Los frameworks distribuidos externos no pueden sustituir el mecanismo central | Nada de etcd, ZooKeeper o brokers. Consenso y replicación son nuestros. |
| Sin APIs de embeddings de pago ni LLM | Vectores sintéticos o de un dataset público. |
| Nombre propio, sin branding de terceros | Nombre genérico. Faiss, Milvus y Weaviate solo como comparación. |

---

## 4. Resumen de garantías

Distinguimos lo que **siempre** se cumple (safety) de lo que se cumple **si hay suficientes
componentes vivos** (liveness/disponibilidad) y de lo que es un **objetivo medido**
(rendimiento).

| Tipo | Garantía |
|---|---|
| Safety | Una escritura confirmada está persistida en una mayoría de réplicas de su shard y no se pierde ante ≤ 2 fallos. |
| Safety | Nunca hay dos responsables confirmando escrituras divergentes en un shard (sin split-brain). |
| Safety | Un reintento con el mismo `request_id` devuelve el resultado original y no reaplica. |
| Safety | Ninguna respuesta muestra un estado a medias (vector y metadatos de versiones distintas). |
| Safety | Un resultado incompleto siempre va marcado. |
| Safety | El top-k con cobertura completa es exacto respecto al estado observado y determinista. |
| Liveness | Con ≤ 2 componentes caídos, las búsquedas tienen cobertura completa y las escrituras se aceptan tras una ventana acotada de reconfiguración. |
| Liveness | Un componente que vuelve se pone al día solo. |
| Liveness | En una partición, el lado con mayoría de un shard sigue escribiendo; la minoría solo sirve lecturas `eventual`. |
| Rendimiento (objetivo) | Búsqueda p99 ≤ 300 ms; escritura p99 ≤ 200 ms; visibilidad eventual p99 ≤ 1 s; failover ≤ 10 s (se calibran en el baseline). |

---

## 5. Decisiones de diseño

Formato de cada decisión: **problema → opciones → elección → por qué aquí → qué cuesta → cómo
lo comprobamos**.

### D01 — Particionado: shards lógicos fijos por hash del id ✅

- **Problema.** Repartir vectores entre nodos de forma que cada escritura sepa a dónde ir y que
  el reparto no cambie cuando cambian los nodos.
- **Opciones:**
  - (a) `hash(id) % N_nodos`.
  - (b) Hashing consistente puro (anillo).
  - (c) Particionar por similitud (clusters, estilo IVF entre nodos).
  - (d) **S shards lógicos fijos** (`shard = hash_estable(colección, id) % S`) más un mapa
    `shard → nodos`.
- **Elección: (d)**, con S fijo (por ejemplo 8) y mayor o igual que el número de nodos.
- **Por qué:**
  - Un vector **nunca cambia de shard**, ni siquiera al actualizarlo, porque su shard depende
    solo del id. Cada escritura toca **un solo shard**, así que el orden por shard basta y no
    hace falta 2PC.
  - Frente a (a): añadir un nodo no obliga a re-hashear todo.
  - Frente a (b): los límites de los shards no se mueven al cambiar la membresía, y eso es
    imprescindible porque cada shard tiene su propia historia ordenada (D05).
  - Frente a (c): reduciría el fan-out, pero un update podría mover un vector a otro shard.
    Esa escritura entre shards dejaría ventanas de "verlo dos veces o ninguna" y crearía
    clusters calientes.
- **Qué cuesta:** toda búsqueda va a todos los shards (§1.3, punto 1). Con S pequeño es
  asumible, y es justo lo que el proyecto quiere estudiar.
- **Detalle:** el hash debe ser **estable entre procesos**. El `hash()` de Python está
  aleatorizado, así que se usa `blake2b`.
- **Comprobación:** unit test de estabilidad y distribución del hash. Test de que `update`
  conserva el shard.

### D02 — Ejecución de consultas: scatter-gather paralelo, top-k local y merge determinista ✅

- **Problema.** Obtener el top-k global consultando S shards sin que la latencia sea la suma de
  todos y sin mover más datos de los necesarios.
- **Elección:**
  - El punto de entrada envía la consulta **en paralelo** a una réplica de cada shard.
  - Cada shard devuelve **solo su top-k local**.
  - El punto de entrada fusiona con un heap ordenando por `(distancia, id)`.
- **Por qué:**
  - El top-k global siempre está contenido en la unión de los top-k locales, así que la fusión
    es exacta.
  - En paralelo, la latencia es el **máximo** de los shards y no la suma (el antipatrón del
    "monolito distribuido").
  - Desempatar por id hace el resultado **determinista** aunque los shards respondan en otro
    orden.
- **Qué cuesta:** S·k candidatos viajan por la red (con S = 8 y k = 10, 80 candidatos).
- **Comprobación:** test de propiedad: merge(top-k locales) = top-k de fuerza bruta global.
  Test de permutación del orden de llegada.

### D03 — Filtros de metadatos dentro del shard (pre-filtrado) ✅

- **Problema.** Si se filtra después de fusionar, pueden quedar menos de k resultados aunque
  existan k que cumplen el filtro.
- **Opciones:** post-filtrado en el punto de entrada (pidiendo k' > k), o filtrado dentro de
  cada shard antes o durante su búsqueda local.
- **Elección:** filtrar **dentro del shard**, restringiendo la búsqueda local a los ids que
  cumplen el filtro.
- **Por qué:** garantiza "exactamente k si existen k que cumplen" (REQ-2.3) y no mueve
  candidatos que se descartarían.
- **Qué cuesta:** cada shard evalúa el filtro sobre sus metadatos (un recorrido o un índice
  invertido simple por campo).
- **Comprobación:** test con filtros muy selectivos, como un solo match en un único shard.

### D04 — Índice local exacto por shard ✅

- **Problema.** Cómo busca cada shard en sus propios vectores.
- **Opciones:** fuerza bruta exacta (NumPy, o un índice plano de una librería como Faiss), o
  índices aproximados (IVF, HNSW).
- **Elección:** **exacto**. Con una colección de prueba de unos 100 000 vectores de dimensión
  128 repartidos en 8 shards, cada shard compara unos 12 500 vectores: milisegundos. ANN queda
  como extensión que no cambia el contrato.
- **Por qué:** con índice exacto, la respuesta completa tiene recall = 1. Así, **cualquier
  pérdida de calidad se debe a la distribución** (shards que faltaron) y no al índice. Eso nos
  da una métrica limpia: *recall@k en modo degradado frente a cobertura*.
- **Qué cuesta:** la búsqueda local es O(n/S·d). No escala a decenas de millones, pero eso está
  fuera del alcance.
- **Comprobación:** recall = 1 con cobertura completa frente a ground truth de fuerza bruta.

### D05 — Replicación y tolerancia a dos fallos ✅ (decisión central, 2026-11-23)

- **Problema.**
  - Cada shard debe sobrevivir a la caída simultánea de **dos componentes cualesquiera**
    (requisito de la defensa) sin perder escrituras confirmadas, sin split-brain y siguiendo
    operativo.
  - Hay que decidir qué se copia, dónde y quién decide quién manda.
- **Restricción matemática:**
  - Un quórum de mayoría con N miembros tolera ⌊(N−1)/2⌋ fallos.
  - Para f = 2 hacen falta **N = 5**.
  - Por eso RF = 3 con W = 2 (o un Raft de 3 nodos) **no basta**: si caen 2 de las 3 réplicas
    de un shard, no queda mayoría y el shard deja de aceptar escrituras.
- **Opciones:**

| Opción | Cómo funciona | A favor | En contra |
|---|---|---|---|
| **A. Raft por shard (RF = 5)** | Cada shard es un grupo de consenso de 5 réplicas. El líder ordena las escrituras y confirma con mayoría (3/5). Si cae el líder, las réplicas eligen otro. | Un solo algoritmo resuelve elección, fencing (término), replicación, puesta al día y truncado de entradas divergentes. Tolera cualquier par de fallos sin componente externo. El paper de Raft (Figura 2) sirve como especificación formal. | 5 copias de cada shard. Un latido por grupo. Lectura `strong` necesita *ReadIndex*. Placement estático. Se aleja del esquema de las conferencias (hay que justificarlo). |
| **B. Plano de control + primario-backup (guía, escalada a f = 2)** | Un plano de control (Raft de 5) guarda el mapa de shards, leases y épocas. Cada shard tiene primario + 4 backups (W = 3/5). El plano de control promueve backups cuando vence un lease. | Es el esquema de las conferencias. Separa metadatos y datos (como Weaviate y Milvus). Permite rebalancear moviendo shards. | **Dos protocolos** a implementar y verificar: Raft y un protocolo de replicación propio con épocas, promoción y truncado, que en la práctica reimplementa la mitad de Raft. Más componentes y más pares de fallos que analizar. |
| **C. Primario-backup RF = 3 "escribir en todas" + servicio de configuración** | Una escritura se confirma cuando está en **todas** las réplicas de la configuración vigente. El servicio de configuración (Raft de 5) saca de la configuración a una réplica caída. | Menos copias (3). Con 2 fallos queda 1 réplica con todo lo confirmado. | Una sola réplica lenta frena todas las escrituras del shard hasta que se reconfigura. Con 2 caídas, la durabilidad queda en 1 copia. También son dos protocolos. |
| D. Sin líder (quórums R + W > N) | Cualquier réplica acepta escrituras. Lectura y escritura por quórum. | Escrituras sin elección. | Escrituras concurrentes sobre el mismo id generan conflictos: hacen falta relojes o vectores de versión, read-repair y anti-entropía. Más difícil de razonar. |

- **Elección: A, Raft por shard con 5 réplicas.**
- **Por qué es la más adecuada aquí:**
  1. **Cumple el requisito de dos fallos de forma directa.** Cualquier par de fallos deja al
     menos 3 de 5 réplicas por shard: mayoría intacta para leer y escribir.
  2. **Menos mecanismos que hacer bien.**
     - La opción B necesita Raft (para el plano de control) y además un protocolo de
       replicación de datos con épocas, promoción del backup con mayor `seq` y truncado.
     - Ese segundo protocolo es, en esencia, Raft con la elección delegada a otro
       componente.
     - Con A implementamos Raft **una vez** y lo instanciamos S veces.
  3. **El coste por escritura es el mismo.**
     - "Consenso por cada vector es caro" no aplica: en estado estable, Raft confirma con un
       viaje de ida y vuelta a la mayoría.
     - Es exactamente lo mismo que un primario-backup que espera a W = 3 de 5.
  4. **Defensa más clara.**
     - Los invariantes de Raft (Election Safety, Log Matching, Leader Completeness, State
       Machine Safety) son conocidos y se pueden comprobar en los tests.
     - Responden directamente a "¿por qué no hay split-brain?" y "¿por qué no vota dos veces?".
- **Qué cuesta:**
  - 5 copias. Unos 51 MB por copia con 100 000 × 128 floats: asumible.
  - Si un nodo muere para siempre, el shard queda con 4 réplicas hasta que vuelva. El
    reemplazo dinámico (cambio de membresía de Raft) es una extensión.
  - Hay que justificar en el informe la diferencia con el esquema de las conferencias. Se
    compara con B en el registro de decisiones.
- **Detalles que se derivan:**
  - El **término** de Raft hace de época (fencing).
  - El **índice del log** es la posición de historia: es el `seq` del token, y no se reinicia
    al cambiar de líder.
  - Las entradas no confirmadas de un líder caído se **truncan** al reconciliar (Log Matching).
- **Comprobación:**
  - Tests deterministas del protocolo con red simulada (particiones, pérdidas, reordenamiento).
  - Checker de invariantes.
  - Campaña F3: fallos del líder, partición 2|3, heal, dos fallos simultáneos.

### D06 — Orden, versiones e identidad sin relojes físicos ✅

- **Problema.** Ordenar escrituras y detectar lo viejo sin fiarse de relojes que pueden
  diferir entre nodos.
- **Elección:** cada concepto tiene un identificador con su propia regla de cambio.

| Identificador | Ámbito | Cuándo cambia | Para qué |
|---|---|---|---|
| `node_id` | nodo | nunca (config) | identidad estable |
| `incarnation` | proceso | en cada arranque (UUID) | distinguir un proceso reiniciado del viejo en logs y estado |
| `term` | shard (grupo) | en cada elección | fencing: rechazar al líder viejo |
| `index` | shard (log) | crece con cada entrada, nunca se reinicia | orden total por shard, tokens RYW, retraso de réplica |
| `request_id` | escritura | uno por escritura lógica (lo genera el cliente) | idempotencia |
| `query_id` | búsqueda | uno por búsqueda (lo genera el punto de entrada) | trazabilidad |

- **Por qué:**
  - Un único escritor por shard (el líder) da orden total sin relojes vectoriales.
  - El tiempo **monotónico** solo se usa para timeouts y latencias; el de pared, solo para
    que los logs sean legibles.
  - En el informe: *"la corrección no depende de relojes físicos"*.
- **Comprobación:** tests de que un líder con término viejo no logra confirmar nada.

### D07 — Idempotencia: `request_id` obligatorio y deduplicación replicada ✅

- **Problema.** Si se pierde la respuesta, el cliente reintenta. `upsert` "parece"
  idempotente, pero no lo es en la historia: si upsert(v1) se reintenta tarde, puede pisar un
  upsert(v2) posterior. `insert` y `update` condicionales no son idempotentes en absoluto.
- **Elección:**
  - Toda escritura lleva un `request_id`.
  - La tabla `request_id → resultado` es **parte del estado replicado** del shard: se aplica
    desde el log.
  - Un `request_id` repetido devuelve el resultado guardado. Si se repite con otro contenido,
    se responde `idempotency_conflict`.
- **Por qué:** al vivir en el log, la deduplicación sobrevive a reinicios y a cambios de líder
  sin mecanismo extra. Además protege contra *replay* de mutaciones autenticadas (F21).
- **Qué cuesta:** memoria por escritura en la tabla. Sin expiración a nuestra escala (pendiente
  de confirmar, C5 de la spec).
- **Comprobación:** escenarios F8 (respuesta perdida) y F9 (entrega duplicada), también tras un
  failover.

### D08 — Visibilidad de escrituras: tres niveles y snapshot por shard ✅

- **Problema.** Con réplicas, la que responde puede ir atrasada. Una garantía del tipo "toda
  escritura confirmada es visible en búsquedas posteriores" sería **falsa** si leemos de
  cualquier réplica.
- **Elección:** el cliente elige el nivel en cada búsqueda.

| Nivel | Garantía por shard | Cómo |
|---|---|---|
| `eventual` (por defecto) | Estado = prefijo de la historia confirmada (sin escrituras a medias); puede ir atrasado | Cualquier réplica sana responde con su estado aplicado |
| `read_your_writes` | Incluye las escrituras cuyos tokens presenta el cliente | La réplica comprueba `aplicado ≥ token`. Si no llega, responde `stale` **sin esperar** y se prueba otra réplica o el líder |
| `strong` | Incluye todo lo confirmado antes de empezar la búsqueda | El líder responde tras confirmar que sigue siéndolo (*ReadIndex*) |

- Y siempre: **no hay snapshot global entre shards**, y se declara.
- **Por qué:**
  - Es honesto: cada nivel dice exactamente qué historias puede observar el cliente.
  - El shard **no espera** dentro de la búsqueda. Si esperara, en un procesador de un solo
    hilo bloquearía justo la escritura que espera (interbloqueo).
  - `eventual` reparte la carga de lectura entre las 5 réplicas.
- **Qué cuesta:** `strong` carga a los líderes y añade un viaje de ida y vuelta de
  confirmación. RYW obliga al cliente a guardar tokens.
- **Comprobación:** experimento "upsert y búsqueda inmediata sin token y con token" y checker
  de historias.

### D09 — Comportamiento degradado: plazo global, timeout por intento, hedge y parciales marcados ✅

- **Problema.** No se puede distinguir un shard lento de uno caído. Esperar sin límite cuelga al
  cliente, y responder incompleto en silencio lo engaña.
- **Elección:**
  - **Plazo global** por búsqueda (300 ms por defecto; el cliente puede ajustarlo).
  - **Timeout por intento** más corto (unos 120 ms, calibrado al p95 de un shard). Si vence, se
    lanza el mismo pedido en otra réplica (*hedge*) y gana la primera respuesta.
  - Al vencer el plazo global se cancela lo pendiente y se responde con lo obtenido, marcado
    con `partial`, `missing_shards` (con el motivo: `unreachable`, `unserved` o `stale`) y
    `coverage`.
  - Con `allow_partial = false`, si falta algo se devuelve un error `unavailable`.
- **Por qué:**
  - Aplica CAP por operación: **las lecturas priorizan disponibilidad** (parcial marcado) y
    **las escrituras priorizan consistencia** (sin mayoría no se escribe).
  - Con 5 réplicas por shard, el hedge casi siempre encuentra otra réplica sana. Los parciales
    quedan para fallos por encima de lo garantizado o para plazos muy cortos.
- **Qué cuesta:** el hedge duplica carga en los casos lentos (se limita a un hedge por shard).
- **Comprobación:** escenarios de nodo lento (tc netem) y de shard indisponible. Se mide
  recall@k frente a cobertura.

### D10 — Durabilidad y recuperación: log persistente con fsync antes de confirmar ✅

- **Problema.** "Confirmado" debe significar que sobrevive a un `kill -9`.
- **Elección:**
  - Cada réplica persiste en disco su log de Raft, su `currentTerm` y su `votedFor`, con
    `fsync` **antes** de responder (a un voto o a un AppendEntries).
  - El líder confirma cuando una mayoría tiene la entrada persistida.
  - Se toman snapshots periódicos de la máquina de estados (índice + metadatos + tabla de
    dedup, etiquetados con su `index`) para arrancar rápido.
  - **Recuperación de una réplica:**
    1. Nueva `incarnation`, estado "no lista".
    2. Carga el snapshot y reaplica el log.
    3. El líder le envía lo que falte.
    4. Cuando alcanza el `commitIndex` del líder, queda **lista** y sirve lecturas.
- **Por qué:** la regla de oro es persistir antes de responder. Responder antes y caerse es
  mentirle al cliente. Docker usa volúmenes, nunca la capa del contenedor.
- **Qué cuesta:** `fsync` en el camino crítico de cada escritura. Si es lento, se agrupan
  varias entradas en un solo `fsync` (group commit).
- **Comprobación:** `kill -9` en mitad de una carga de inserciones; se verifica que todo lo
  confirmado sigue ahí. Se mide el tiempo hasta "lista".

### D11 — Descubrimiento y membresía: bootstrap estático + líderes por grupo ✅

- **Problema.** Cómo encuentra cada proceso a los demás y quién manda en cada shard.
- **Opciones:** configuración estática, registro con leases, gossip/SWIM o DHT.
- **Elección:**
  - **Lista estática de nodos y placement estático** de réplicas en un fichero de
    configuración versionado, idéntico en todos los nodos.
  - El líder de cada shard lo determina Raft. El punto de entrada lo aprende de las respuestas
    `not_leader` (que incluyen la pista del líder) y de un sondeo periódico de estado.
- **Por qué:**
  - Con 5 nodos, todos conocen el mapa entero y el lookup es de un salto. Una DHT (Chord,
    Kademlia) no aporta nada.
  - Gossip crearía una segunda fuente de verdad potencialmente desactualizada.
  - Lo único dinámico que importa (quién es el líder) ya lo resuelve el consenso.
  - Se declara: alta y baja de nodos es una extensión (cambio de membresía de Raft).
- **Detección de fallos:**
  - Los followers detectan la caída del líder por su timeout de elección.
  - El punto de entrada marca réplicas como sospechosas por timeouts. Eso **solo cambia el
    enrutamiento** y nunca borra estado: un timeout es sospecha, no certeza.
- **Alternativa descartada con D05:** registro con leases y heartbeats dentro de un plano de
  control (ALIVE → SUSPECT → DEAD). Con Raft por shard no hace falta: la detección de fallos
  del líder la hace el propio grupo.
- **Colecciones (C3):** se declaran en la misma configuración (nombre, dimensión, métrica,
  permisos), así que no hace falta un catálogo dinámico replicado. La API ya usa
  `/collections/{c}/...`: añadir la creación por API más adelante no cambiaría el contrato.

### D12 — Concurrencia dentro de cada réplica: un solo hilo de aplicación ✅

- **Problema.** Que una búsqueda no vea un estado a medias mientras se aplica una escritura.
- **Elección:**
  - Cada réplica es **dueña exclusiva** de su índice y sus metadatos (*shared-nothing*).
  - Las operaciones (aplicar una entrada, buscar) se procesan en **una cola acotada y un único
    hilo**, como un actor.
- **Por qué:**
  - Cada búsqueda ve un estado consistente "gratis", sin locks y sin filtrar por versión dentro
    del índice.
  - Es el mismo modelo que un actor de Ray en F1, así que el núcleo no cambia entre fases.
- **Qué cuesta:**
  - Las búsquedas esperan detrás de las escrituras de esa réplica, y al revés.
  - Si hiciera falta más paralelismo: lock lectura/escritura o copy-on-write del índice.
    NumPy y Faiss liberan el GIL en el cálculo pesado.
- **Comprobación:** tests de concurrencia upsert + search sobre el mismo id (nunca mezcla).

### D13 — Backpressure: admisión acotada y rechazo explícito ✅

- **Problema.** Sin límites, una sobrecarga llena colas y memoria y luego todo cae junto.
- **Elección:**
  - Semáforo de búsquedas concurrentes en cada punto de entrada.
  - Cola de comandos acotada en cada réplica.
  - Al llenarse, se rechaza **de inmediato** con `overloaded` (HTTP 429).
  - Límite de tasa por cliente.
  - El cliente de referencia reintenta con backoff exponencial, jitter y un máximo de reintentos.
- **Por qué:** fallar rápido es preferible a degradar a todos. Además, los reintentos sin
  control amplifican la sobrecarga (F17).
- **Comprobación:** escenarios F16 (sobrecarga sostenida) y F17 (amplificación de reintentos).

### D14 — Comunicación: REST/JSON hacia fuera, RPC binario hacia dentro, sin broker ✅

- **Problema.** Qué protocolo conecta a los clientes y a los procesos entre sí.
- **Elección:**
  - **Cliente → punto de entrada:** HTTP + JSON. Cualquier cliente (curl, Python) lo usa, y es
    idéntico en F1 y F2.
  - **Interno (F2):** gRPC + protobuf:
    - Contrato tipado.
    - Deadlines nativos.
    - Vectores como bytes `float32` little-endian.
    - Campo de versión en cada mensaje.
  - **F1:** llamadas a actores de Ray.
  - **Sin broker** de mensajes.
- **Por qué:**
  - La replicación ya es un log propio. Un broker añadiría un componente que operar y, peor,
    **sustituiría el mecanismo** que el proyecto debe demostrar.
  - JSON para vectores internos multiplicaría el tamaño de los mensajes y el coste de
    serialización.
- **Comprobación:** tests de contrato idénticos contra F1 y F2.

### D15 — Puntos de entrada sin estado y replicados; failover en el cliente ✅

- **Problema.** El punto de entrada (coordinador de consultas) no puede ser un componente único.
- **Elección:**
  - Varios puntos de entrada **sin estado**, uno por nodo. Solo guardan una caché de quién es
    el líder de cada shard.
  - El SDK del cliente tiene la lista de todos y cambia de uno a otro si falla. Como las
    escrituras son idempotentes (D07), reintentar en otro es seguro.
- **Por qué:** un balanceador único sería otro punto único de fallo. Con 5 puntos de entrada,
  cualquier par de fallos deja 3.
- **Comprobación:** matar dos puntos de entrada durante una carga; el cliente sigue.

### D16 — Seguridad mínima defendible ✅

- **Modelo de amenazas:**
  - **Activos:** vectores y metadatos.
  - **Principales:** clientes con API key.
  - **Frontera de confianza:** cliente → punto de entrada.
  - **Supuesto declarado:** la red interna es confiable.
  - **Fuera de alcance:** nodos bizantinos y atacantes dentro del host.
- **Mecanismos:**
  - API key por cliente.
  - Permisos de lectura y escritura **por colección**, aplicados en el servidor. El aislamiento
    nunca se confía a un filtro que manda el cliente.
  - Límites de entrada: `k` máximo, dimensión exacta, tamaño de metadatos y de lote,
    complejidad del filtro.
  - Rate limit por cliente.
  - El `request_id` frena el replay.
  - Nunca se escriben en los logs credenciales ni vectores completos.
- **Comprobación:** escenarios F20 (operación no autorizada) y F21 (replay).

### D17 — Observabilidad desde el primer día ✅

- **Logs estructurados (JSON).** Cada evento lleva:
  - `node`, `incarnation`, `shard`, `role`, `term`, `index`.
  - Tipo de evento.
  - `request_id` / `query_id`.
  
  Con esto se reconstruye el orden real **sin fiarse de las horas ni del orden de las líneas**.
- **Métricas:**
  - Latencia p50/p95/p99 global y por shard (nunca solo la media).
  - Rechazos por admisión.
  - Respuestas parciales.
  - Retraso de replicación (`commitIndex` del líder − `appliedIndex` de cada réplica).
  - Elecciones y falsas sospechas.
- **Historias de cliente.** El cliente de pruebas registra `(cliente, op, t_inv, t_resp,
  resultado)`. Un **checker** las valida contra el contrato.
- **Por qué:** la Fase 3 exige evidencia legible por máquina, no capturas.

### D18 — Lenguaje y stack de la Fase 2 ✅ (2026-11-23)

| Opción | A favor | En contra |
|---|---|---|
| **Python** (elegido) | Dominio numérico (NumPy/Faiss). Reutiliza el núcleo de shard, el SDK y los tests de F1. La comparación Ray/nativo mantiene el lenguaje constante, así que las diferencias se deben al middleware. Velocidad de desarrollo. | Rendimiento y GIL: hay que cuidar no bloquear el event loop. La metodología marca Go como opción "normal" y hay que justificar no usarlo. |
| Go | Opción normal del curso. Concurrencia real y binarios simples. Buen soporte de gRPC. | Reescribir núcleo, filtros y tests. Matemática vectorial a mano o con gonum. La comparación con Ray mezcla el efecto del lenguaje y el del middleware. |

- **Elección:** Python 3.11+, asyncio, grpcio y NumPy (Faiss opcional).
- **Justificación para el informe:** el dominio es numérico y el núcleo se comparte entre
  fases. Ningún framework distribuido esconde mecanismos: Raft, replicación, detección de
  fallos y enrutamiento son código nuestro.

### D19 — Despliegue y modelo de nodo ✅

- **Elección:** Docker Compose en una máquina.
  - **F1:** un clúster Ray (head + 2–3 workers en contenedores).
  - **F2:** **5 nodos**. Cada nodo es un contenedor que aloja sus procesos (nodo de
    almacenamiento con sus réplicas y punto de entrada). Así, "caída de un nodo" = matar el
    contenedor y "caída de un proceso" = matar un proceso dentro de él, que es justo como la
    metodología define los fallos.
  - Los datos van en volúmenes persistentes.
- **Inyección de fallos:**

| Fallo | Cómo se provoca |
|---|---|
| Crash | `docker kill` / `kill -9` |
| Pausa | `docker pause` |
| Retraso o pérdida de paquetes | `tc netem` (requiere `NET_ADMIN`) |
| Partición | `docker network disconnect` o `iptables` |

- **Qué se declara:** los contenedores **simulan** dominios de fallo, pero comparten kernel,
  disco y CPU con la máquina.

---

## 6. Lo que deliberadamente no hacemos (y por qué, en una línea)

- **Chord o Kademlia:** con 5 nodos todos conocen el mapa entero y el lookup es de un salto.
- **Bully o algoritmo en anillo:** no evitan el split-brain. La elección de Raft sí, gracias a
  la mayoría y los términos.
- **Paxos:** equivalente en garantías a Raft. Elegimos Raft porque es más fácil de implementar
  y de explicar.
- **Relojes vectoriales:** hay un único escritor por shard, así que no existen escrituras
  concurrentes sobre una misma clave.
- **Cristian o Berkeley:** no sincronizamos relojes porque la corrección no depende de ellos.
- **Gossip/SWIM:** sería una segunda fuente de verdad; con membresía estática y consenso sobra.
- **2PC/sagas:** ninguna operación toca más de un shard.
- **Broker de mensajes:** sustituiría el mecanismo de replicación que debemos mostrar.
- **CRDT:** no aceptamos escrituras desconectadas; la minoría no escribe.
- **Snapshot global entre shards:** costaría coordinación entre shards en cada búsqueda, y el
  problema no la necesita.

---

## 7. Cómo cambia la arquitectura entre fases

El **contrato observable** es igual en las dos fases. Cambia quién hace cada cosa por dentro.

| Responsabilidad | Fase 1 (Ray) | Fase 2 (nativo) |
|---|---|---|
| Ejecución remota | Métodos de actor (`.remote()`) | RPC gRPC explícitas |
| Estado del shard | Actor `ShardReplica` en memoria | Réplica dentro del proceso del nodo: log persistente + snapshot |
| Directorio (dónde está cada réplica) | Named actors (GCS de Ray) | Configuración estática |
| Quién manda en cada shard | Primario fijo por shard, publicado por el actor `Catalog` | Elección de Raft por grupo |
| Replicación | El primario asigna `seq`, reenvía a los backups y confirma con W acks; el resto se pone al día de forma asíncrona (todo en memoria) | Log de Raft persistente, mayoría 3/5 |
| Placement | Scheduler de Ray (estrategia SPREAD) | Placement estático en la configuración |
| Esperar con timeout | `ray.wait(timeout=…)` | Deadlines de gRPC + `asyncio.wait` |
| Supervisión y reinicio | `max_restarts` (**reinicia con el estado vacío**) | Reinicio del contenedor + recuperación desde disco + puesta al día |
| Serialización | Pickle/Arrow, transparente | Protobuf, vectores `<f4` en bytes |
| Serialización de operaciones en el shard | Buzón del actor (un hilo) | Cola de comandos de la réplica (un hilo) |
| Observabilidad | Dashboard y logs de Ray + nuestros logs | Nuestros logs, métricas e historias |

La pregunta que guía la Fase 2: **¿qué capacidades distribuidas necesita realmente este
problema?** Implementamos solo esas. No reconstruimos un scheduler general, ni un object store,
ni autoscaling.

---

## 8. Vista general de la Fase 2

```
                         Clientes (SDK/CLI con lista de puntos de entrada)
                                   │  HTTP/JSON (API key)
          ┌──────────────┬─────────┼─────────┬──────────────┐
          ▼              ▼         ▼         ▼              ▼
     ┌─────────┐   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
     │ nodo-1  │   │ nodo-2  │ │ nodo-3  │ │ nodo-4  │ │ nodo-5  │
     │ gateway │   │ gateway │ │ gateway │ │ gateway │ │ gateway │  ← sin estado
     │ storage │   │ storage │ │ storage │ │ storage │ │ storage │  ← réplicas de shards
     │  s0..s7 │   │  s0..s7 │ │  s0..s7 │ │  s0..s7 │ │  s0..s7 │
     │ [disco] │   │ [disco] │ │ [disco] │ │ [disco] │ │ [disco] │
     └────┬────┘   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
          └─────────────┴──── gRPC ─┴───────────┴───────────┘
     gateway → storage: Search / Write / Get / Status
     storage ↔ storage: RequestVote / AppendEntries (un grupo Raft por shard)
     Líderes repartidos: cada nodo lidera ~S/5 shards
```

Con 5 nodos y RF = 5, cada nodo aloja una réplica de cada shard. El sharding sigue repartiendo
dos cosas:

- **El trabajo de cada búsqueda:** cada uno de los S sub-pedidos va a una réplica distinta y
  recorre 1/S de los datos.
- **El liderazgo de escritura:** cada shard tiene su propio líder.

Con más de 5 nodos, cada uno alojaría solo una parte de los shards.

---

## 9. Glosario

| Término | Definición |
|---|---|
| **Shard** | Subconjunto fijo de vectores, determinado por `hash(colección, id) % S`. Es la unidad de orden y de degradación. |
| **Réplica** | Copia de un shard en un nodo. Un grupo Raft por shard. |
| **Líder** | Réplica que ordena y confirma las escrituras del shard en un término. |
| **Término** | Número de "mandato" de Raft. Sirve como época para el fencing. |
| **Índice** | Posición en el log del shard. Es el `seq` del token de escritura. |
| **Escritura confirmada** | Persistida en la mayoría (3 de 5) y aplicada por el líder. |
| **Cobertura** | Shards que respondieron / S. |
| **Hedge** | Reenviar un pedido lento a otra réplica y quedarse con la primera respuesta. |
| **Punto de entrada (gateway)** | Proceso sin estado que recibe peticiones, hace scatter-gather y encamina escrituras al líder. |

---

> **Aviso de referencias de terceros.** Faiss, Milvus, Weaviate, Ray y otros sistemas se
> mencionan solo con fines descriptivos, históricos, técnicos y educativos. Sus nombres, marcas
> y logos pertenecen a sus titulares. Este proyecto se desarrolla de forma independiente y no
> está afiliado, patrocinado ni respaldado por ellos, ni es una implementación oficial o
> compatible de ninguno.
