from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_dirs(fecha: str) -> tuple[Path, Path]:
    #---(infra) Rutas---- Construye `data/<fecha>/` y `data/<fecha>/output/` segun `.config`.
    from .config import cfg_get

    data_dir = cfg_get("paths.data_dir", "data")
    output_dir_name = cfg_get("paths.output_dir_name", "output")

    base_dir = repo_root() / str(data_dir) / fecha
    output_dir = base_dir / str(output_dir_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, output_dir
