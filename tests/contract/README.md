# Tests de contrato HTTP (T119)

`test_http_contract.py` habla HTTP con `httpx` y verifica únicamente respuestas observables del
contrato. No importa el backend, la API FastAPI ni Ray. La misma suite se puede ejecutar contra
el stub de T119b o el gateway Ray. Genera IDs únicos por prueba y no asume cantidad ni nombres de
shards.

## Contra el stub

En una terminal desde la raíz del repositorio:

```powershell
make stub
```

En otra terminal:

```powershell
$env:DVS_BASE_URL = "http://127.0.0.1:8080"
$env:DVS_API_KEY = "dev-key-rw"
make test-contract
```

`test_stub_server.py` conserva las pruebas de integración específicas de T119b que construyen
el stub en proceso. La suite HTTP nueva es independiente de esa implementación.

## Contra el clúster Ray

Después de `make up-ray`, establece `DVS_BASE_URL` y `DVS_API_KEY` como arriba y ejecuta
`make test-contract`. Para la configuración local del proyecto se usan `dev-key-rw`, `dev-key-ro`
y `dev-admin`; la clave de solo lectura y la clave admin se pueden cambiar con
`DVS_READONLY_API_KEY` y `DVS_ADMIN_API_KEY`.

Las pruebas marcadas `faults` se omiten por defecto. Para ejecutarlas, prepara en el clúster una
falla que deje al menos un shard sin respuesta y establece `DVS_FAULTS_EXPECT_PARTIAL=1`; esas
pruebas comprueban tanto los parciales explícitos como el error `unavailable` cuando se desactivan.
