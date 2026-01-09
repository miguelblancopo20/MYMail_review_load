from __future__ import annotations

import logging
import unicodedata
from pathlib import Path

import pandas as pd
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from .config import cfg_get

logger = logging.getLogger(__name__)


def _norm_text(value: object) -> str:
    txt = "" if value is None else str(value)
    txt = "".join(c for c in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(c))
    return txt.strip().lower()


def _sanitize_excel_text_series(series: pd.Series) -> pd.Series:
    # Limpia caracteres ilegales para Excel (control chars) y limita a 32.767 chars por celda.
    def _clean(v: object) -> str:
        s = "" if v is None else str(v)
        s = ILLEGAL_CHARACTERS_RE.sub("", s)
        return s[:32767]

    return series.apply(_clean)


def export_categoria_no_contemplada_y_otros(df_ia: pd.DataFrame, output_dir: Path) -> None:
    # Paso a paso:
    # 1) Validar columnas minimas.
    # 2) Filtrar `Sublocation` == "Categoría no contemplada" y exportar.
    # 3) Filtrar `Sublocation` == "Otros", exportar y crear Pivot por `Location` (count idLotus).
    # 4) Guardar Excels en `output_dir`.
    required_order = ["@timestamp", "Sublocation", "idLotus", "Question", "MailToAgent", "Location", "Subject"]

    if "Sublocation" not in df_ia.columns:
        logger.warning("IA filtros: falta columna `Sublocation`; no se generan Categoria_No_Contemplada/Otros.")
        return
    if "idLotus" not in df_ia.columns:
        logger.warning("IA filtros: falta columna `idLotus`; no se generan Categoria_No_Contemplada/Otros.")
        return

    sub_norm = df_ia["Sublocation"].apply(_norm_text)

    df_cat = df_ia[sub_norm.eq("categoria no contemplada")].copy()
    if not df_cat.empty:
        for col in required_order:
            if col not in df_cat.columns:
                df_cat[col] = ""
        df_cat_out = df_cat[required_order].copy()
        for col in required_order:
            df_cat_out[col] = _sanitize_excel_text_series(df_cat_out[col])
        out_cat = output_dir / str(cfg_get("outputs.categoria_no_contemplada_xlsx", "Categoría_No_Contemplada.xlsx"))
        with pd.ExcelWriter(out_cat, engine="openpyxl") as writer:
            df_cat_out.to_excel(writer, sheet_name="Data", index=False)
        logger.info("IA filtros: OK -> %s (rows=%s)", out_cat, df_cat_out.shape[0])
    else:
        logger.info("IA filtros: Categoria no contemplada sin filas (0)")

    df_otros = df_ia[sub_norm.eq("otros")].copy()
    if not df_otros.empty:
        for col in required_order:
            if col not in df_otros.columns:
                df_otros[col] = ""
        df_otros_out = df_otros[required_order].copy()
        for col in required_order:
            df_otros_out[col] = _sanitize_excel_text_series(df_otros_out[col])
        out_otros = output_dir / str(cfg_get("outputs.otros_xlsx", "Otros.xlsx"))
        df_pivot_src = df_otros.copy()
        if "Location" in df_pivot_src.columns:
            df_pivot_src["Location"] = df_pivot_src["Location"].fillna("(blank)").astype(str).replace({"": "(blank)"})
        else:
            df_pivot_src["Location"] = "(blank)"

        pivot = (
            df_pivot_src.pivot_table(
                index="Location",
                values="idLotus",
                aggfunc="count",
                fill_value=0,
                dropna=False,
            )
            .reset_index()
            .rename(columns={"idLotus": "Count_idLotus"})
        )

        with pd.ExcelWriter(out_otros, engine="openpyxl") as writer:
            df_otros_out.to_excel(writer, sheet_name="Data", index=False)
            pivot.to_excel(writer, sheet_name="Pivot", index=False)
        logger.info("IA filtros: OK -> %s (rows=%s)", out_otros, df_otros_out.shape[0])
    else:
        logger.info("IA filtros: Otros sin filas (0)")

def load_inputs(folder: str, segmento_csv: str):
    ia_csv = f"{folder}/{cfg_get('inputs.ia_csv', 'ia.csv')}"
    rpa_csv = f"{folder}/{cfg_get('inputs.rpa_csv', 'rpa.csv')}"

    df_ia = pd.read_csv(ia_csv, sep=";")
    df_segmento = pd.read_csv(segmento_csv, on_bad_lines="skip")
    df_rpa = pd.read_csv(rpa_csv)
    return df_ia, df_segmento, df_rpa


def build_merged_df(df_ia: pd.DataFrame, df_rpa: pd.DataFrame) -> pd.DataFrame:
    df_rpa = df_rpa.drop_duplicates(subset=["IDU"], keep="first").copy()
    df_rpa["IDU"] = df_rpa["IDU"].fillna("").astype(str)
    df_rpa = df_rpa.drop_duplicates(subset=["IDU"]).copy()
    df_rpa = df_rpa[df_rpa["IDU"].isin(df_ia["idLotus"])].copy()

    merged_df = pd.merge(df_ia, df_rpa, left_on="idLotus", right_on="IDU", how="inner")
    merged_df = merged_df.drop_duplicates(subset=["IDU"])

    merged_df["Sublocation"] = merged_df.apply(
        lambda x: "Duplicado de factura" if "Duplicado de factura" in str(x.Sublocation) else x.Sublocation,
        axis=1,
    )
    merged_df["Location"] = merged_df.apply(
        lambda x: "Categoría no contemplada" if "Categoría no contemplada" in str(x.Location) else x.Location,
        axis=1,
    )
    return merged_df


def compute_tables(merged_df: pd.DataFrame, df_segmento: pd.DataFrame) -> dict:
    sublocation_counts = merged_df["Sublocation"].value_counts()
    location_counts = merged_df["Location"].value_counts()
    status_counts = merged_df["Status"].value_counts()

    sublocation_counts_df = sublocation_counts.reset_index().rename(
        columns={"index": "Sublocation", "Sublocation": "Count"}
    )
    location_counts_df = location_counts.reset_index().rename(columns={"index": "Location", "Location": "Count"})
    status_counts_df = status_counts.reset_index().rename(columns={"index": "Status", "Status": "Count"})

    merged_with_segmento = pd.merge(
        merged_df, df_segmento, left_on="Documento", right_on="NUMERO DOCUMENTO", how="left"
    )

    total_segment_counts = merged_with_segmento["SEGMENTO"].value_counts(dropna=False)
    total_segment_counts_df = total_segment_counts.reset_index().rename(
        columns={"index": "SEGMENTO", "SEGMENTO": "Count"}
    )

    seg_x_subtematica = (
        merged_with_segmento.groupby(["Sublocation", "SEGMENTO"]).size().unstack(fill_value=0).reset_index()
    )
    seg_x_tematica = (
        merged_with_segmento.groupby(["Location", "SEGMENTO"]).size().unstack(fill_value=0).reset_index()
    )

    return {
        "merged_df": merged_with_segmento,
        "mails_subtematica": sublocation_counts_df,
        "mails_tematica": location_counts_df,
        "mails_status": status_counts_df,
        "cifs_segmento_total": total_segment_counts_df,
        "seg_x_subtematica": seg_x_subtematica,
        "seg_x_tematica": seg_x_tematica,
    }


def write_excel(output_excel: str, tables: dict) -> None:
    resumen_df = pd.DataFrame(
        [
            {"Metric": "Mails Totales (rows, cols)", "Value": str(tables["merged_df"].shape)},
            {"Metric": "Output Excel", "Value": output_excel},
        ]
    )

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        resumen_df.to_excel(writer, sheet_name="Resumen", index=False)
        tables["mails_subtematica"].to_excel(writer, sheet_name="Mails_Subtematica", index=False)
        tables["mails_tematica"].to_excel(writer, sheet_name="Mails_Tematica", index=False)
        tables["mails_status"].to_excel(writer, sheet_name="Mails_Status", index=False)
        tables["cifs_segmento_total"].to_excel(writer, sheet_name="CIFs_Segmento_Total", index=False)
        tables["seg_x_subtematica"].to_excel(writer, sheet_name="Seg_x_Subtematica", index=False)
        tables["seg_x_tematica"].to_excel(writer, sheet_name="Seg_x_Tematica", index=False)


def run(folder: str, segmento_csv: str, output: str | None) -> None:
    #---5. Subtematicas---- Cruza `ia.csv` + `rpa.csv`, segmenta y genera `mails_por_subtematica.xlsx`.
    logger.info("Subtematicas: inicio (folder=%s)", folder)
    output_dir = Path(folder) / str(cfg_get("paths.output_dir_name", "output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    if output:
        output_path = Path(output)
        if not output_path.is_absolute() and str(output_path.parent) == ".":
            output_path = output_dir / output_path.name
    else:
        output_path = output_dir / str(cfg_get("outputs.subtematicas_xlsx", "mails_por_subtematica.xlsx"))

    df_ia, df_segmento, df_rpa = load_inputs(folder, segmento_csv)

    # Los Excels `Categoría_No_Contemplada.xlsx` y `Otros.xlsx` salen del detalle `ia-transacciones.csv`
    # para asegurar que `Question/MailToAgent/Subject` vengan completos.
    ia_transacciones_path = f"{folder}/{cfg_get('inputs.ia_transacciones_csv', 'ia-transacciones.csv')}"
    try:
        df_ia_trans = pd.read_csv(
            ia_transacciones_path,
            sep=";",
            usecols=["@timestamp", "Sublocation", "idLotus", "Question", "MailToAgent", "Location", "Subject"],
            dtype=str,
            keep_default_na=False,
        )
        export_categoria_no_contemplada_y_otros(df_ia_trans, output_dir)
    except FileNotFoundError:
        logger.warning("IA filtros: no existe %s; no se generan Categoria_No_Contemplada/Otros.", ia_transacciones_path)
    except Exception as exc:
        logger.warning("IA filtros: fallo generando Excels desde %s (%s)", ia_transacciones_path, exc)

    merged_df = build_merged_df(df_ia, df_rpa)

    tables = compute_tables(merged_df, df_segmento)

    write_excel(str(output_path), tables)
    logger.info("Subtematicas: OK -> %s (rows=%s)", output_path, tables["merged_df"].shape[0])
