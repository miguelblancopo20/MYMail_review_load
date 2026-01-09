from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .config import cfg_get
from .paths import get_dirs

logger = logging.getLogger(__name__)


def _stable_unique_join(series: pd.Series) -> str:
    seen: list[str] = []
    for v in series.fillna("").astype(str).tolist():
        v = v.strip()
        if not v or v in seen:
            continue
        seen.append(v)
    return " | ".join(seen)


def generar_all_xlsx(fecha: str) -> None:
    #---7. All---- Cruza validaciones + ia-transacciones + ejecuciones + fichas_levantadas y genera `all.xlsx` (1 fila por correo).
    # Paso a paso:
    # 1) Leer los Excels generados (`validaciones.xlsx`, `ejecuciones.xlsx`, `fichas_levantadas.xlsx`).
    # 2) Normalizar claves de correo y deduplicar a 1 fila por correo en cada fuente.
    # 3) Agregar fichas/orquestador a columnas resumen para mantener 1 fila por correo.
    # 4) Cruce final (outer join) y orden de columnas.
    # 5) Exportar a `data/<fecha>/output/all.xlsx`.
    base_dir, output_dir = get_dirs(fecha)
    out_path = output_dir / str(cfg_get("outputs.all_xlsx", "all.xlsx"))

    validaciones_path = output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx"))
    ejecuciones_path = output_dir / str(cfg_get("outputs.ejecuciones_xlsx", "ejecuciones.xlsx"))
    fichas_path = output_dir / str(cfg_get("outputs.fichas_levantadas_xlsx", "fichas_levantadas.xlsx"))

    def _read_xlsx(path: Path, sheet: str) -> pd.DataFrame:
        return pd.read_excel(path, sheet_name=sheet)

    logger.info("All: inicio (fecha=%s)", fecha)

    try:
        df_val = _read_xlsx(validaciones_path, "Data")
    except Exception as exc:
        logger.warning("All: no se pudo leer %s (Data): %s", validaciones_path, exc)
        df_val = pd.DataFrame()

    try:
        df_eje = _read_xlsx(ejecuciones_path, "Data")
    except Exception as exc:
        logger.warning("All: no se pudo leer %s (Data): %s", ejecuciones_path, exc)
        df_eje = pd.DataFrame()

    try:
        df_fichas = _read_xlsx(fichas_path, "Data")
    except Exception as exc:
        logger.warning("All: no se pudo leer %s (Data): %s", fichas_path, exc)
        df_fichas = pd.DataFrame()

    if df_val.empty and df_eje.empty and df_fichas.empty:
        logger.warning("All: sin datos de entrada (validaciones/ejecuciones/fichas) -> no se genera %s", out_path)
        return

    # (A) Validaciones base
    if "IdCorreo" in df_val.columns:
        df_val["IdCorreo"] = df_val["IdCorreo"].fillna("").astype(str).str.split("-").str[0]
        # asegurar 1 fila por correo (si el merge con ia-transacciones generase duplicados)
        if "@timestamp" in df_val.columns:
            ts = pd.to_datetime(df_val["@timestamp"], errors="coerce")
            df_val = df_val.assign(_ts=ts).sort_values("_ts", ascending=False).drop(columns=["_ts"])
        df_val = df_val.drop_duplicates(subset=["IdCorreo"], keep="first")

    # (B) Ejecuciones (solo columnas propias)
    df_eje_agg = pd.DataFrame()
    if not df_eje.empty and "IdCorreo Validacion" in df_eje.columns:
        df_eje = df_eje.copy()
        df_eje["IdCorreo"] = df_eje["IdCorreo Validacion"].fillna("").astype(str).str.split("-").str[0]

        df_eje_agg = (
            df_eje.groupby("IdCorreo", dropna=False)
            .agg(
                Ejecucion_Encontrados=("Encontrados", _stable_unique_join)
                if "Encontrados" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_IDU_contexto=("IDU contexto", _stable_unique_join)
                if "IDU contexto" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_Automatismos=("Automatismo", _stable_unique_join)
                if "Automatismo" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_Segmentos=("Segmento", _stable_unique_join)
                if "Segmento" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_Documentos=("Documento", _stable_unique_join)
                if "Documento" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_Matriculas=("MatriculaAsesor", _stable_unique_join)
                if "MatriculaAsesor" in df_eje.columns
                else ("IdCorreo", lambda s: ""),
                Ejecucion_rows=("IdCorreo", "size"),
            )
            .reset_index()
        )

    # (C) Fichas levantadas (agregado para mantener 1 fila por correo)
    df_fichas_agg = pd.DataFrame()
    if not df_fichas.empty and "IdCorreo" in df_fichas.columns:
        df_fichas = df_fichas.copy()
        df_fichas["IdCorreo"] = df_fichas["IdCorreo"].fillna("").astype(str).str.split("-").str[0]

        if "@timestamp" in df_fichas.columns:
            df_fichas["_ts"] = pd.to_datetime(df_fichas["@timestamp"], errors="coerce", utc=False)
        df_fichas_agg = (
            df_fichas.groupby("IdCorreo", dropna=False)
            .agg(
                Fichas_rows=("IdCorreo", "size"),
                Fichas_Timestamp_Min=("_ts", "min") if "_ts" in df_fichas.columns else ("IdCorreo", lambda s: ""),
                Fichas_Timestamp_Max=("_ts", "max") if "_ts" in df_fichas.columns else ("IdCorreo", lambda s: ""),
                Fichas_Automatismos=("Automatismo", _stable_unique_join) if "Automatismo" in df_fichas.columns else ("IdCorreo", lambda s: ""),
                Fichas_Segmentos=("Segmento", _stable_unique_join) if "Segmento" in df_fichas.columns else ("IdCorreo", lambda s: ""),
                Fichas_Documentos=("Documento", _stable_unique_join) if "Documento" in df_fichas.columns else ("IdCorreo", lambda s: ""),
                Fichas_Matriculas=("MatriculaAsesor", _stable_unique_join)
                if "MatriculaAsesor" in df_fichas.columns
                else ("IdCorreo", lambda s: ""),
            )
            .reset_index()
        )
        if "Fichas_Timestamp_Min" in df_fichas_agg.columns:
            df_fichas_agg["Fichas_Timestamp_Min"] = df_fichas_agg["Fichas_Timestamp_Min"].fillna("").astype(str)
        if "Fichas_Timestamp_Max" in df_fichas_agg.columns:
            df_fichas_agg["Fichas_Timestamp_Max"] = df_fichas_agg["Fichas_Timestamp_Max"].fillna("").astype(str)
        df_fichas_agg["Tiene_Ficha"] = df_fichas_agg["Fichas_rows"].apply(lambda n: "SI" if int(n or 0) > 0 else "NO")

    # (D) Cruce final
    if not df_val.empty and "IdCorreo" in df_val.columns:
        df_all = df_val.copy()
    else:
        ids = pd.Index([])
        for df, col in [(df_eje_agg, "IdCorreo"), (df_fichas_agg, "IdCorreo")]:
            if not df.empty and col in df.columns:
                ids = ids.union(df[col].fillna("").astype(str))
        df_all = pd.DataFrame({"IdCorreo": ids})

    if not df_eje_agg.empty:
        df_all = df_all.merge(df_eje_agg, on="IdCorreo", how="outer")
    if not df_fichas_agg.empty:
        df_all = df_all.merge(df_fichas_agg, on="IdCorreo", how="outer")

    # Orden de columnas: IdCorreo primero, luego validaciones, luego ejecuciones, luego fichas.
    first = ["IdCorreo"]
    exec_cols = [c for c in df_all.columns if c.startswith("Ejecucion_")]
    fichas_cols = [c for c in df_all.columns if c.startswith("Fichas_") or c in {"Tiene_Ficha"}]
    other = [c for c in df_all.columns if c not in set(first + exec_cols + fichas_cols)]
    ordered = first + other + exec_cols + fichas_cols
    df_all = df_all[ordered]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_all.to_excel(writer, sheet_name="Data", index=False)
    except PermissionError as exc:
        logger.warning("All: no se pudo escribir %s (esta abierto en Excel?): %s", out_path, exc)
        return
    except OSError as exc:
        logger.warning("All: error escribiendo %s: %s", out_path, exc)
        return

    logger.info("All: OK -> %s (rows=%s)", out_path, df_all.shape[0])
