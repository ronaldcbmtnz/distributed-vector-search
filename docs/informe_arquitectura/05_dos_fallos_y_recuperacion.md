# 5. Dos fallos y recuperación

> ⬜ **Esqueleto.** **Qué pide la metodología:**
> - Justificar por qué el diseño cubre **cualquier par** de componentes. Se pueden agrupar
>   pares que se comportan igual, si se explica por qué.
> - Para un **par representativo**, seguir la misma petición de la sección 4 durante los fallos
>   y la recuperación.
> - Describir qué observaría el cliente y cómo se comprueba que conserva la funcionalidad
>   prometida.
>
> Recordatorio: el fallo de un nodo afecta a **todos** los procesos que aloja.

## 5.1 Argumento general

_(Raft por shard, RF = 5: cada shard tiene 5 réplicas en 5 nodos distintos. Cualquier par de fallos —dos
procesos, dos nodos o proceso + nodo— elimina como mucho 2 réplicas de cada shard, así que
quedan ≥ 3 = mayoría. Con 5 gateways quedan ≥ 3. No existe ningún componente único.)_

## 5.2 Clases de pares

| Clase | Ejemplos | Efecto inmediato | Qué lo cubre | Ventana de indisponibilidad | Qué observa el cliente |
|---|---|---|---|---|---|
| Gateway + gateway | | | | | |
| Gateway + proceso de almacenamiento | | | | | |
| 2 procesos de almacenamiento (followers de todos los shards afectados) | | | | | |
| 2 procesos de almacenamiento, incluido el líder de algún shard | | | | | |
| 2 nodos completos | | | | | |
| Proceso + nodo distinto | | | | | |

**Por qué cada agrupación es válida:** _(pendiente)_

## 5.3 Par representativo: caen a la vez el nodo con el líder del shard s y otro nodo

| Instante | Evento | Estado del sistema | Lo que ve el cliente |
|---|---|---|---|
| t0 | Carga normal: upsert + búsqueda RYW (sección 4) | | |
| t1 | `docker kill` de los nodos X e Y | | |
| t1 + δ | Gateways: timeouts → sospecha → hedge a otras réplicas | | |
| t1 + election timeout | Elección en el shard s; nuevo líder con término + 1 | | |
| … | Escrituras reintentadas con el mismo `request_id` | | |
| t2 | Vuelven X e Y: recuperación desde disco y puesta al día | | |
| t3 | Réplicas "listas"; RF efectivo = 5 de nuevo | | |

## 5.4 Cómo se comprueba

_(escenario de la campaña, métricas: tiempo hasta aceptar escrituras de nuevo, cobertura de
búsquedas durante la ventana, checker de historias sin pérdidas ni duplicados; enlace a la
evidencia)_

## 5.5 Más allá de lo garantizado

_(qué pasa con 3 fallos: shards sin mayoría rechazan escrituras; búsquedas parciales marcadas o
`eventual` desde réplicas vivas; sin corrupción. Se muestra como límite de la garantía.)_
