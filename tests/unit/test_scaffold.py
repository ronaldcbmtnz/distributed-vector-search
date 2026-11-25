"""Guardas de arquitectura del andamiaje (T008).

Comprueban un principio OBLIGATORIO de la constitución: solo `dvs.ray_impl` (y el punto de
arranque `dvs.api.main_ray`) pueden importar Ray. Así el núcleo y el coordinador se reutilizan
tal cual en la Fase 2, donde Ray está prohibido.
"""

import pkgutil
import subprocess
import sys

import dvs

RAY_ALLOWED = ("dvs.ray_impl", "dvs.api.main_ray")


def test_package_version() -> None:
    assert dvs.__version__


def _all_modules() -> list[str]:
    return [m.name for m in pkgutil.walk_packages(dvs.__path__, prefix="dvs.")]


def test_only_ray_impl_imports_ray() -> None:
    modules = [m for m in _all_modules() if not m.startswith(RAY_ALLOWED)]
    code = (
        "import importlib, sys\n"
        f"for m in {modules!r}:\n"
        "    importlib.import_module(m)\n"
        "print('ray' in sys.modules)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "False", "un módulo fuera de dvs.ray_impl importa ray"
