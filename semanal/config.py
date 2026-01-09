from __future__ import annotations

import json
from pathlib import Path

_CONFIG: dict | None = None


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def set_config(config: dict) -> None:
    global _CONFIG
    _CONFIG = config


def get_config() -> dict:
    if _CONFIG is None:
        raise RuntimeError("Config no cargada. Llama a load_config()/set_config() desde CLI.")
    return _CONFIG


def load_config(config_path: str | None = None) -> dict:
    #---(infra) Config---- Carga `.config` (JSON) y deja el config disponible via `cfg_get()`.
    root = repo_root()
    candidates = []
    if config_path:
        candidates.append(Path(config_path))
    else:
        candidates.extend([root / ".config", root / "semanal.config"])

    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        checked = ", ".join(str(p) for p in candidates)
        raise FileNotFoundError(f"No existe fichero de config. Rutas comprobadas: {checked}")

    if not path.is_absolute():
        path = (root / path).resolve()

    config = json.loads(path.read_text(encoding="utf-8"))
    set_config(config)
    return config


def cfg_get(path: str, default=None):
    """
    Accessor con notacion tipo: 'inputs.validaciones_csv' o 'paths.data_dir'.
    """
    cur = get_config()
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def resolve_repo_path(value: str | None) -> Path | None:
    if value is None:
        return None
    p = Path(value)
    return p if p.is_absolute() else (repo_root() / p).resolve()
