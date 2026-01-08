from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .config import cfg_get

logger = logging.getLogger(__name__)

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
        lambda x: "Categorヴa no contemplada" if "Categorヴa no contemplada" in x.Location else x.Location,
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
    merged_df = build_merged_df(df_ia, df_rpa)

    print("Mails Totales:\n" + str(merged_df.shape) + "\n")
    print("Mails desglosados por subtematica:\n" + str(merged_df["Sublocation"].value_counts()))
    print("Mails desglosados por tematica:\n" + str(merged_df["Location"].value_counts()))
    print("Mails desglosados por Status:\n" + str(merged_df["Status"].value_counts()))

    tables = compute_tables(merged_df, df_segmento)

    print("Total de CIFs por segmento:\n" + str(tables["merged_df"]["SEGMENTO"].value_counts(dropna=False)))
    print(
        "Conteo de CIFs de cada segmento en cada subtematica:\n"
        + str(tables["merged_df"].groupby(["Sublocation", "SEGMENTO"]).size().unstack(fill_value=0))
    )
    print(
        "Conteo de CIFs de cada segmento en cada tematica:\n"
        + str(tables["merged_df"].groupby(["Location", "SEGMENTO"]).size().unstack(fill_value=0))
    )

    write_excel(str(output_path), tables)
    print(f"\nGuardado Excel con tablas en: {output_path}")
    logger.info("Subtematicas: OK -> %s (rows=%s)", output_path, tables["merged_df"].shape[0])
