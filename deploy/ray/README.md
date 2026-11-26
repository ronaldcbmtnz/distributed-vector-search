# Despliegue Ray de Fase 1

El archivo `docker-compose.yml` levanta un clúster Ray local con un head, tres workers y un
gateway HTTP. La imagen usa `rayproject/ray:2.60.1-py312-cpu`, la misma versión de Ray y Python
que declara `pyproject.toml`; la imagen instala `.[ray]` y un SDK OpenTelemetry compatible con
FastAPI, y ejecuta `pip check` durante el build para rechazar dependencias inconsistentes.

## Arranque y apagado

Desde la raíz del repositorio, con Docker Engine y Docker Compose disponibles:

```sh
make up-ray
make down-ray
```

`make up-ray` construye la imagen, espera a que head y workers estén saludables, espera la salud
HTTP del gateway y muestra `ray status`. Además, una consulta al estado de Ray comprueba que hay
exactamente cuatro nodos vivos antes de terminar correctamente. El gateway se conecta al servidor
Ray Client del head (`DVS_RAY_ADDRESS=ray://ray-head:10001`); no ejecuta `ray start` ni agrega un
quinto nodo.
Este detalle mantiene la aceptación de T118 de cuatro nodos Ray (head + tres workers).

Los servicios publican el gateway en `http://127.0.0.1:8080` por defecto, y el dashboard del head en
`http://127.0.0.1:8265`. Los puertos GCS `6379` y Ray Client `10001` quedan disponibles solo dentro
de la red Docker.
Los cuatro procesos Ray usan `shm_size: 2gb`; los tres workers reciben `NET_ADMIN` para los
escenarios de red que usan `tc netem`.

## Configuración y evidencia

- El gateway monta `config.ray.yaml` en solo lectura como `DVS_CONFIG`.
- Los servicios comparten el directorio local `runs/` en `/runs`.
- `DVS_RUN_ID` selecciona el subdirectorio de la ejecución; por defecto es `compose`.
- La configuración incluida trae claves de desarrollo. No se deben usar fuera del entorno local.
- No se monta estado de Ray en disco: la configuración F1 es en memoria y no promete failover ni
  persistencia de escrituras.

Para cambiar el identificador de ejecución en PowerShell:

```powershell
$env:DVS_RUN_ID = "t118-local"
make up-ray
```

Si el puerto local 8080 ya está ocupado, se puede elegir otro puerto anfitrión (el puerto dentro
del contenedor sigue siendo 8080):

```powershell
$env:DVS_GATEWAY_PORT = "8081"
make up-ray
```

Comprobaciones manuales útiles:

```sh
docker compose -f deploy/ray/docker-compose.yml ps
docker compose -f deploy/ray/docker-compose.yml exec ray-head ray status --address=127.0.0.1:6379
curl http://127.0.0.1:8080/v1/health
```

Para ejecutar las pruebas HTTP contra el gateway, establece `DVS_BASE_URL` y `DVS_API_KEY` y
ejecuta `make test-contract`. La suite no depende de Ray ni inspecciona nombres o cantidad de
shards.
