from __future__ import annotations

import json
import logging

import pandas as pd

from .config import cfg_get
from .excel_utils import (
    aplicar_estilos_validaciones_excel,
    reordenar_pivot_blank,
    reordenar_pivot_blank_multiindex,
)
from .paths import get_dirs

logger = logging.getLogger(__name__)


def fichas_levantadas(fecha: str) -> None:
    #---2. Fichas levantadas---- Lee `fichas_levantadas.csv` y genera `fichas_levantadas.xlsx` (Data + Pivot).
    logger.info("Fichas levantadas: inicio (fecha=%s)", fecha)
    base_dir, output_dir = get_dirs(fecha)
    df = pd.read_csv(base_dir / str(cfg_get("inputs.fichas_levantadas_csv", "fichas_levantadas.csv")))

    df["IdCorreo"] = df["IdCorreo"].apply(lambda x: x.split("-")[0])
    df = df.drop_duplicates(subset=["IdCorreo", "Automatismo"], keep="first")

    output_path = output_dir / str(cfg_get("outputs.fichas_levantadas_xlsx", "fichas_levantadas.xlsx"))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)

        if "Segmento" in df.columns:
            df_pivot_src = df.copy()
            df_pivot_src["Segmento"] = df_pivot_src["Segmento"].fillna("(blank)").astype(str).replace({"": "(blank)"})
            df_pivot_src["Automatismo"] = (
                df_pivot_src["Automatismo"].fillna("(blank)").astype(str).replace({"": "(blank)"})
            )
            df_pivot_src["IdCorreo"] = df_pivot_src["IdCorreo"].fillna("(blank)").astype(str).replace({"": "(blank)"})

            pivot = df_pivot_src.pivot_table(
                index="Automatismo",
                columns="Segmento",
                values="IdCorreo",
                aggfunc="count",
                fill_value=0,
                dropna=False,
            )
            pivot = reordenar_pivot_blank(pivot, blank_label="(blank)", after_column="PE")

            pivot["Total"] = pivot.sum(axis=1)
            pivot.loc["Total"] = pivot.sum(axis=0)
            pivot = reordenar_pivot_blank(pivot, blank_label="(blank)", after_column="PE", keep_total_last=True)
            pivot = pivot.reset_index()
            pivot.to_excel(writer, sheet_name="Pivot", index=False)
        else:
            pd.DataFrame(
                [{"WARN": "No existe la columna 'Segmento' en fichas_levantadas.csv; no se genera Pivot."}]
            ).to_excel(writer, sheet_name="Pivot", index=False)
    logger.info("Fichas levantadas: OK -> %s (rows=%s)", output_path, df.shape[0])


def validados(fecha: str) -> None:
    #---1. Validaciones---- Enriquecer con `ia-transacciones.csv`, y generar `validaciones.xlsx` (Data + Pivot + estilos).
    logger.info("Validaciones: inicio (fecha=%s)", fecha)
    base_dir, output_dir = get_dirs(fecha)
    df = pd.read_csv(base_dir / str(cfg_get("inputs.validaciones_csv", "validaciones.csv")))
    df = df.drop_duplicates(subset=["IdCorreo"], keep="first")

    df_ia = pd.read_csv(base_dir / str(cfg_get("inputs.ia_transacciones_csv", "ia-transacciones.csv")), sep=";")
    if "@timestamp" in df_ia.columns:
        df_ia = df_ia.rename(columns={"@timestamp": "IA_timestamp"})
    if "IA_timestamp" not in df_ia.columns:
        df_ia["IA_timestamp"] = ""
    df_ia = df_ia[
        [
            "idLotus",
            "IA_timestamp",
            "Location",
            "Sublocation",
            "Subject",
            "Question",
            "MailToAgent",
        ]
    ]
    df_ia = df_ia.drop_duplicates(subset=["idLotus"], keep="first")

    df = pd.merge(df, df_ia, how="left", left_on="IdCorreo", right_on="idLotus")
    df["Faltan datos?"] = df.apply(lambda x: faltan_datos(x.Automatismo, x.MailToAgent), axis=1)

    df.sort_values(by=["Automatismo", "@timestamp"], ascending=[True, False], inplace=True)
    output_path = output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx"))
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)

        df_pivot_src = df.copy()
        if "Segmento" in df_pivot_src.columns:
            df_pivot_src["Segmento"] = df_pivot_src["Segmento"].fillna("Sin Segmento")
        if "Validado" in df_pivot_src.columns:
            df_pivot_src["Validado"] = df_pivot_src["Validado"].fillna("Sin Validado")

        if all(col in df_pivot_src.columns for col in ["Automatismo", "Validado", "Segmento", "IdCorreo"]):
            pivot = df_pivot_src.pivot_table(
                index="Automatismo",
                columns=["Validado", "Segmento"],
                values="IdCorreo",
                aggfunc="count",
                fill_value=0,
            )
            pivot.columns = [f"{validado} | {segmento}" for validado, segmento in pivot.columns.to_list()]
            pivot = pivot.reset_index()
            pivot.to_excel(writer, sheet_name="Pivot", index=False)
        else:
            pd.DataFrame(
                [{"WARN": "Faltan columnas para generar Pivot (Automatismo, Validado, Segmento, IdCorreo)."}]
            ).to_excel(writer, sheet_name="Pivot", index=False)

    aplicar_estilos_validaciones_excel(output_path)
    logger.info("Validaciones: OK -> %s (rows=%s)", output_path, df.shape[0])


def faltan_datos(automatismo, mta) -> bool:
    if "Apertura" not in str(automatismo):
        return False

    try:
        contexto = json.loads(mta)
        parametros = contexto["Parametros"]
        return not all([parametros.get("PERSONA CONTACTO"), parametros.get("TELEFONO CONTACTO")])
    except Exception:
        return False


def ejecuciones(fecha: str) -> None:
    #---3. Ejecuciones---- Cruza VALIDADO vs `orquestador_contexto.csv` y genera `ejecuciones.xlsx` (Data + Pivot).
    logger.info("Ejecuciones: inicio (fecha=%s)", fecha)
    base_dir, output_dir = get_dirs(fecha)
    df_validaciones = pd.read_excel(
        output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx")), sheet_name="Data"
    )
    df_validaciones = df_validaciones.drop_duplicates(subset=["IdCorreo"], keep="first")
    df_validaciones = df_validaciones[
        ["IdCorreo", "Validado", "Documento", "Automatismo", "Segmento", "MatriculaAsesor"]
    ]
    df_validaciones = df_validaciones[df_validaciones["Validado"] == "VALIDADO"]

    automatismos = sorted(df_validaciones["Automatismo"].dropna().unique().tolist())

    df_orquestador_contexto = pd.read_csv(
        base_dir / str(cfg_get("inputs.orquestador_contexto_csv", "orquestador_contexto.csv"))
    )
    df_orquestador_contexto["IDU"] = df_orquestador_contexto["IDU"].apply(lambda x: str(x).split("-")[0])
    df_orquestador_contexto = df_orquestador_contexto.drop_duplicates(subset=["IDU"], keep="first")[["IDU"]]

    cruces = []

    for automatismo in automatismos:
        df_auto = df_validaciones[df_validaciones["Automatismo"] == automatismo]
        if df_auto.empty:
            continue

        df_cruce = pd.merge(df_auto, df_orquestador_contexto, how="left", left_on="IdCorreo", right_on="IDU")
        df_cruce = df_cruce.drop_duplicates(subset=["IdCorreo"], keep="first")
        df_cruce = df_cruce.rename(columns={"IdCorreo": "IdCorreo Validacion", "IDU": "IDU contexto"})
        cruces.append(df_cruce)

        total_validados = df_cruce["IdCorreo Validacion"].nunique()
        ejecutados = df_cruce["IDU contexto"].notna().sum()
        porcentaje = (100 * ejecutados / total_validados) if total_validados else 0

        logger.info(
            "Ejecuciones: %s | Validados=%s Ejecutados=%s Porcentaje=%.2f",
            automatismo,
            total_validados,
            ejecutados,
            porcentaje,
        )

    output_path = output_dir / str(cfg_get("outputs.ejecuciones_xlsx", "ejecuciones.xlsx"))

    if not cruces:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="Data", index=False)
            pd.DataFrame([{"WARN": "No hay registros VALIDADO para generar ejecuciones/pivot."}]).to_excel(
                writer, sheet_name="Pivot", index=False
            )
        return

    df_out = pd.concat(cruces, ignore_index=True)
    df_out = df_out[["IdCorreo Validacion", "Automatismo", "Segmento", "Documento", "MatriculaAsesor", "IDU contexto"]]
    df_out["Encontrados"] = df_out["IDU contexto"].apply(lambda x: "SI" if pd.notna(x) else "NO")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, sheet_name="Data", index=False)

        df_pivot_src = df_out.copy()
        df_pivot_src["Automatismo"] = (
            df_pivot_src["Automatismo"].fillna("(blank)").astype(str).replace({"": "(blank)"})
        )
        df_pivot_src["Segmento"] = df_pivot_src["Segmento"].fillna("(blank)").astype(str).replace({"": "(blank)"})
        df_pivot_src["Encontrados"] = (
            df_pivot_src["Encontrados"].fillna("(blank)").astype(str).replace({"": "(blank)"})
        )
        df_pivot_src["IdCorreo Validacion"] = (
            df_pivot_src["IdCorreo Validacion"].fillna("(blank)").astype(str).replace({"": "(blank)"})
        )

        pivot = df_pivot_src.pivot_table(
            index="Automatismo",
            columns=["Encontrados", "Segmento"],
            values="IdCorreo Validacion",
            aggfunc="count",
            fill_value=0,
            dropna=False,
        )
        pivot = reordenar_pivot_blank_multiindex(pivot, blank_label="(blank)", after_segment="PE")

        pivot[("Total", "")] = pivot.sum(axis=1)
        pivot.loc["Total"] = pivot.sum(axis=0)

        pivot = reordenar_pivot_blank_multiindex(pivot, blank_label="(blank)", after_segment="PE")

        pivot.columns = [
            "Total" if encontrados == "Total" else f"{encontrados} | {segmento}"
            for encontrados, segmento in pivot.columns.to_list()
        ]
        pivot = pivot.reset_index().rename(columns={"index": "Automatismo"})
        pivot.to_excel(writer, sheet_name="Pivot", index=False)
    logger.info("Ejecuciones: OK -> %s (rows=%s)", output_path, df_out.shape[0])
