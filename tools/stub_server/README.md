# T119b — Stub del contrato HTTP

El stub ejecuta el API existente con un backend en memoria, dentro de un único proceso. Reutiliza
la validación, autenticación, permisos y formato de errores de `dvs.api`; el backend aplica las
escrituras de forma síncrona y calcula las búsquedas exactas mediante fuerza bruta. No usa Ray, no
se distribuye y no simula fallos ni resultados parciales.

Desde la raíz del repositorio, con el entorno Python 3.12 instalado:

```bash
make stub
```

Por defecto escucha en `127.0.0.1:8080` y carga `deploy/ray/config.local.yaml`. Se puede elegir
otra configuración con `DVS_CONFIG`, y cambiar el host o puerto con `DVS_STUB_HOST` y
`DVS_STUB_PORT`. Para la configuración local, las claves de desarrollo están documentadas en
`docs/REPARTO_TAREAS.md` §3.1. El estado se reinicia al detener el proceso. Comprueba el arranque
con `curl http://127.0.0.1:8080/v1/health`.

El runner de tests de contrato de T119 podrá apuntar a este servicio mediante `DVS_BASE_URL` y
`DVS_API_KEY`, conforme a `docs/REPARTO_TAREAS.md` §3.1.
