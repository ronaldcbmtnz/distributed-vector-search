# Distributed Vector Search

Prototipo de búsqueda vectorial distribuida para la asignatura de Sistemas Distribuidos. La
Fase 1 usa FastAPI y Ray; `tools/stub_server` ofrece un backend en memoria para desarrollo y
pruebas del contrato sin levantar Ray.

## Cliente Python y CLI

Con Python 3.12 y las dependencias instaladas (`pip install -e ".[dev,ray]"`), configura la URL y
la clave de API:

```powershell
# Levanta antes `make stub` o `make up-ray` en otra terminal.
$env:DVS_BASE_URL = "http://127.0.0.1:8080"
$env:DVS_API_KEY = "dev-key-rw"
python -c "import json; open('vector.json', 'w', encoding='utf-8').write(json.dumps([1.0] + [0.0] * 127))"
dvs health
dvs upsert demo item-1 --vector "@vector.json" --metadata '{"source":"demo"}'
dvs search demo --vector "@vector.json" --k 10
make data
dvs load demo --data-dir data/demo --batch-size 500
```

Los vectores deben tener la dimensión configurada para la colección. El SDK síncrono y el
asíncrono están disponibles en `dvs.client`; ambos generan `request_id`, reintentan solo fallos
transitorios con backoff y jitter, aceptan varios gateways separados por coma en `DVS_BASE_URL`,
gestionan tokens RYW y pueden registrar historias JSONL con `history_path` o `DVS_HISTORY_PATH`.

```python
from dvs.client import AsyncDVSClient

async with AsyncDVSClient() as client:
    await client.upsert("demo", "item-2", [1.0] + [0.0] * 127)
    result = await client.search("demo", [1.0] + [0.0] * 127, consistency="read_your_writes")
```

## Servicios y pruebas

- `make stub`: inicia el servidor simulado de T119b en `127.0.0.1:8080`.
- `make up-ray` / `make down-ray`: construyen, levantan y apagan el clúster de T118; ver
  [deploy/ray/README.md](deploy/ray/README.md).
- `make test-contract`: ejecuta las pruebas HTTP contra `DVS_BASE_URL` y las pruebas específicas
  de T119b.
- `make check`: Ruff, mypy del núcleo/coordinador y tests unitarios.
