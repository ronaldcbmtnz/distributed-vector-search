# Tests de contrato HTTP (T119)

`test_http_contract.py` habla HTTP con `httpx` y verifica únicamente respuestas observables del
contrato (`docs/fase1_ray/01_contrato_api.md`). Genera IDs únicos por prueba, limpia lo que crea
y no asume cantidad ni nombres de shards. La misma suite se ejecuta sin cambios contra el stub
(T119b), el gateway Ray de la Fase 1 y, más adelante, el sistema nativo de la Fase 2.

## Destino de las pruebas

- **Sin `DVS_BASE_URL`**: se ejecutan contra el stub **en proceso** (no hace falta levantar nada).
- **Con `DVS_BASE_URL`**: caja negra pura contra ese servidor.

```bash
make test-contract                                            # stub en proceso
DVS_BASE_URL=http://127.0.0.1:8080 make test-contract         # servidor real (make run-ray-local / make up-ray)
```

En PowerShell:

```powershell
$env:DVS_BASE_URL = "http://127.0.0.1:8080"
make test-contract
```

Claves: `DVS_API_KEY` (por defecto `dev-key-rw`), `DVS_READONLY_API_KEY` (`dev-key-ro`) y
`DVS_ADMIN_API_KEY` (`dev-admin`), las acordadas en `docs/REPARTO_TAREAS.md` §3.1.

## Pruebas con fallos

Las pruebas marcadas `faults` se omiten por defecto. Para ejecutarlas, prepara en el clúster un
shard sin ninguna réplica viva (por ejemplo, `python -m dvs.ray_impl.faults kill --replica sN/rI
--no-restart` para sus tres réplicas) y define `DVS_FAULTS_EXPECT_PARTIAL=1`. Comprueban el
parcial explícito (`partial`, `coverage`, `missing_shards`) y el `503 unavailable` con
`allow_partial=false`.

Las pruebas en proceso del stub y del generador de datos están en `tests/unit/`
(`test_stub_server.py`, `test_data_gen.py`) y se ejecutan con `make check`.
