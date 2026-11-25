# Informe de arquitectura (sin Ray)

> **Entrega:** 1 ene 2027, 23:59 UTC−4.
> **Qué pide la metodología:** un informe que explique **en profundidad la arquitectura sin Ray
> y cada uno de sus componentes**, siguiendo el "Modelo del informe de arquitectura".
>
> Se divide en un archivo por sección del modelo para trabajar en paralelo. Para la entrega se
> concatenan en un único documento (p. ej. con `pandoc` a PDF) en este orden.

## Estructura (modelo de la metodología)

| # | Sección | Qué debe contener | Archivo | Estado |
|---|---|---|---|---|
| 1 | **Servicio y garantías** | Clientes, operaciones que necesitan y resultados observables que promete el sistema | [`01_servicio_y_garantias.md`](01_servicio_y_garantias.md) | ⬜ |
| 2 | **Arquitectura sin Ray** | Nodos, procesos de cada uno y canales. Para cada responsabilidad que asumía Ray: quién la asume ahora y cómo | [`02_arquitectura_sin_ray.md`](02_arquitectura_sin_ray.md) | ⬜ |
| 3 | **Ficha por componente** | Responsabilidad, ubicación, E/S, estado, dependencias, protocolo; cómo decide, por qué ese diseño, cómo responde al fallo propio o de una dependencia | [`03_fichas_componentes.md`](03_fichas_componentes.md) | ⬜ |
| 4 | **Operación de extremo a extremo** | Una petición propia, del cliente al resultado: mensajes, decisiones y cambios de estado en cada paso | [`04_operacion_extremo_a_extremo.md`](04_operacion_extremo_a_extremo.md) | ⬜ |
| 5 | **Dos fallos y recuperación** | Por qué el diseño cubre cualquier par de componentes (se pueden agrupar pares). Un par representativo traza a traza: qué observa el cliente y cómo se comprueba | [`05_dos_fallos_y_recuperacion.md`](05_dos_fallos_y_recuperacion.md) | ⬜ |
| A | Registro de decisiones | Tabla Requisito \| Fase 1 Ray \| Alternativas nativas \| Diseño elegido \| Razón/compromiso | [`06_registro_decisiones.md`](06_registro_decisiones.md) | ⬜ |

## Reglas de redacción

- "Es consistente" o "es disponible" sin definir no cuenta. Cada garantía se formula como
  **historias observables** (qué puede ver un cliente y qué no).
- Cada decisión se presenta como **compromiso**, no como "lo correcto".
- Mencionar un algoritmo del curso (Raft, quórum…) **nunca** basta como justificación: hay que
  decir qué garantía aporta aquí.
- Cada afirmación apunta a su evidencia: test, experimento o línea de código.
- Si algo queda centralizado o es SPOF, se declara y se explica por qué es aceptable.
- Un mecanismo omitido se justifica (p. ej. por qué no hay gossip, 2PC ni CRDT).
- Al final va el aviso de referencias de terceros (Faiss, Milvus, Weaviate, Ray).

## Checklist final

- [ ] Las 5 secciones del modelo y el registro de decisiones están completos.
- [ ] Todas las decisiones arquitectónicas de la metodología (comunicación, identidad,
  discovery, membresía, propiedad, durabilidad, replicación, consistencia, reintentos,
  detección de fallos, partición, recuperación, reconciliación, scheduling, backpressure,
  observabilidad, seguridad) tienen respuesta o justificación de omisión.
- [ ] Los diagramas usan cajas y etiquetas neutrales, sin logos.
- [ ] El análisis de pares de fallos cubre **todas** las clases de pares.
- [ ] Aviso de terceros incluido.
