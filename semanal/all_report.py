from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from .config import cfg_get
from .paths import get_dirs

logger = logging.getLogger(__name__)

IA_COLS_ORDER = [
    "idLotus",
    "IA_timestamp",
    "Location",
    "Sublocation",
    "Subject",
    "Question",
    "MailToAgent",
]

_ILLEGAL_XLSX_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _stable_unique_join(series: pd.Series) -> str:
    seen: list[str] = []
    for v in series.fillna("").astype(str).tolist():
        v = v.strip()
        if not v or v in seen:
            continue
        seen.append(v)
    return " | ".join(seen)


def _sanitize_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        df[col] = df[col].map(lambda v: _ILLEGAL_XLSX_CHARS_RE.sub("", v) if isinstance(v, str) else v)
    return df


def _read_ia_transacciones(base_dir: Path) -> pd.DataFrame:
    ia_path = base_dir / str(cfg_get("inputs.ia_transacciones_csv", "ia-transacciones.csv"))
    try:
        df_ia = pd.read_csv(ia_path, sep=";", low_memory=False)
    except FileNotFoundError:
        logger.warning("All: no existe %s; se sigue sin base IA", ia_path)
        return pd.DataFrame()
    except Exception as exc:
        logger.warning("All: no se pudo leer %s: %s", ia_path, exc)
        return pd.DataFrame()

    if df_ia.empty:
        return df_ia

    if "@timestamp" in df_ia.columns:
        df_ia = df_ia.rename(columns={"@timestamp": "IA_timestamp"})
    if "IA_timestamp" not in df_ia.columns:
        df_ia["IA_timestamp"] = ""

    if "idLotus" not in df_ia.columns:
        logger.warning("All: %s no tiene columna 'idLotus'; se sigue sin base IA", ia_path)
        return pd.DataFrame()

    df_ia = df_ia.copy()
    df_ia["IdCorreo"] = df_ia["idLotus"].fillna("").astype(str).str.split("-").str[0]

    # Mantener solo columnas relevantes si existen (para reducir peso del merge).
    keep = ["IdCorreo"] + [c for c in IA_COLS_ORDER if c in df_ia.columns]
    extra = [c for c in df_ia.columns if c not in set(keep)]
    if extra:
        df_ia = df_ia[keep]

    if "IA_timestamp" in df_ia.columns:
        ts = pd.to_datetime(df_ia["IA_timestamp"], errors="coerce", format="mixed")
        df_ia = df_ia.assign(_ia_ts=ts).sort_values("_ia_ts", ascending=False).drop(columns=["_ia_ts"])
    df_ia = df_ia.drop_duplicates(subset=["IdCorreo"], keep="first")
    return df_ia


def generar_all_xlsx(fecha: str) -> None:
    #---7. All---- Cruza validaciones + ia-transacciones + ejecuciones + fichas_levantadas y genera `all.xlsx` (1 fila por correo).
    # Paso a paso:
    # 1) Leer `ia-transacciones.csv` (base) y los Excels generados (`validaciones.xlsx`, `ejecuciones.xlsx`, `fichas_levantadas.xlsx`).
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

    df_ia = _read_ia_transacciones(base_dir)

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

    if df_ia.empty and df_val.empty and df_eje.empty and df_fichas.empty:
        logger.warning("All: sin datos de entrada (ia/validaciones/ejecuciones/fichas) -> no se genera %s", out_path)
        return

    # (A) Validaciones (sin columnas IA, porque `all` parte de `ia-transacciones.csv`)
    if "IdCorreo" in df_val.columns:
        df_val["IdCorreo"] = df_val["IdCorreo"].fillna("").astype(str).str.split("-").str[0]
        if "@timestamp" in df_val.columns:
            ts = pd.to_datetime(df_val["@timestamp"], errors="coerce")
            df_val = df_val.assign(_ts=ts).sort_values("_ts", ascending=False).drop(columns=["_ts"])
        df_val = df_val.drop_duplicates(subset=["IdCorreo"], keep="first")
        drop_ia_cols = [c for c in dict.fromkeys(["idLotus", *IA_COLS_ORDER]) if c in df_val.columns]
        if drop_ia_cols:
            df_val = df_val.drop(columns=drop_ia_cols)

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
    started_from_ia = False
    if not df_ia.empty and "IdCorreo" in df_ia.columns:
        df_all = df_ia.copy()
        started_from_ia = True
    elif not df_val.empty and "IdCorreo" in df_val.columns:
        df_all = df_val.copy()
    else:
        ids = pd.Index([])
        for df, col in [(df_eje_agg, "IdCorreo"), (df_fichas_agg, "IdCorreo")]:
            if not df.empty and col in df.columns:
                ids = ids.union(df[col].fillna("").astype(str))
        df_all = pd.DataFrame({"IdCorreo": ids})

    # IA como base (si existe) o como enriquecimiento (si no se pudo usar de base).
    if not df_ia.empty and not started_from_ia:
        df_all = df_all.merge(df_ia, on="IdCorreo", how="outer")

    # Enriquecer con validaciones (manteniendo la base IA si aplica).
    if not df_val.empty:
        df_all = df_all.merge(df_val, on="IdCorreo", how="left" if started_from_ia else "outer")

    if not df_eje_agg.empty:
        df_all = df_all.merge(df_eje_agg, on="IdCorreo", how="outer")
    if not df_fichas_agg.empty:
        df_all = df_all.merge(df_fichas_agg, on="IdCorreo", how="outer")

    # Orden de columnas: IdCorreo, IA, validaciones, ejecuciones, fichas.
    first = ["IdCorreo"]
    ia_cols = [c for c in IA_COLS_ORDER if c in df_all.columns]
    exec_cols = [c for c in df_all.columns if c.startswith("Ejecucion_")]
    fichas_cols = [c for c in df_all.columns if c.startswith("Fichas_") or c in {"Tiene_Ficha"}]
    other = [c for c in df_all.columns if c not in set(first + ia_cols + exec_cols + fichas_cols)]
    ordered = first + ia_cols + other + exec_cols + fichas_cols
    df_all = df_all[ordered]

    # Leyenda: origen (fichero) por columna.
    ia_src = f"data/{fecha}/{cfg_get('inputs.ia_transacciones_csv', 'ia-transacciones.csv')}"
    validaciones_csv_src = f"data/{fecha}/{cfg_get('inputs.validaciones_csv', 'validaciones.csv')}"
    orquestador_ctx_src = f"data/{fecha}/{cfg_get('inputs.orquestador_contexto_csv', 'orquestador_contexto.csv')}"
    fichas_csv_src = f"data/{fecha}/{cfg_get('inputs.fichas_levantadas_csv', 'fichas_levantadas.csv')}"

    validaciones_xlsx_src = f"{output_dir / str(cfg_get('outputs.validaciones_xlsx', 'validaciones.xlsx'))}"
    ejecuciones_xlsx_src = f"{output_dir / str(cfg_get('outputs.ejecuciones_xlsx', 'ejecuciones.xlsx'))}"
    fichas_xlsx_src = f"{output_dir / str(cfg_get('outputs.fichas_levantadas_xlsx', 'fichas_levantadas.xlsx'))}"

    def _origin_for_col(col: str) -> str:
        if col == "IdCorreo":
            return ia_src if started_from_ia else validaciones_csv_src
        if col in {"idLotus", *IA_COLS_ORDER}:
            return ia_src
        if col in df_val.columns:
            return f"{validaciones_xlsx_src} (derivado de {validaciones_csv_src} + {ia_src})"
        if col.startswith("Ejecucion_"):
            return f"{ejecuciones_xlsx_src} (derivado de {validaciones_xlsx_src} + {orquestador_ctx_src})"
        if col.startswith("Fichas_") or col == "Tiene_Ficha":
            return f"{fichas_xlsx_src} (derivado de {fichas_csv_src})"
        return "calculado en all_report.py"

    df_leyenda = pd.DataFrame(
        [{"Columna": c, "Origen": _origin_for_col(str(c))} for c in df_all.columns]
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            _sanitize_df_for_excel(df_all).to_excel(writer, sheet_name="Data", index=False)
            _sanitize_df_for_excel(df_leyenda).to_excel(writer, sheet_name="Leyenda", index=False)
    except PermissionError as exc:
        logger.warning("All: no se pudo escribir %s (esta abierto en Excel?): %s", out_path, exc)
        return
    except OSError as exc:
        logger.warning("All: error escribiendo %s: %s", out_path, exc)
        return

    logger.info("All: OK -> %s (rows=%s)", out_path, df_all.shape[0])
