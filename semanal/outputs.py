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

    # A nivel de negocio: consolidamos el listado de fichas levantadas para poder medir volumen por automatismo y segmento.
    df = pd.read_csv(base_dir / str(cfg_get("inputs.fichas_levantadas_csv", "fichas_levantadas.csv")))

    # A nivel de negocio: normalizamos el identificador del correo para evitar duplicidades por sufijos del propio proceso.
    df["IdCorreo"] = df["IdCorreo"].apply(lambda x: x.split("-")[0])
    # A nivel de negocio: si para un mismo correo hay una fila "en blanco" de automatismo y otra con automatismo informado,
    # descartamos la fila "en blanco" para no degradar el análisis (nos quedamos con la fila con información).
    if "Automatismo" in df.columns:
        automatismo_txt = df["Automatismo"].fillna("").astype(str).str.strip()
        is_blank_auto = automatismo_txt.eq("") | automatismo_txt.eq("(blank)")
        has_non_blank_auto = (~is_blank_auto).groupby(df["IdCorreo"]).transform("any")
        df = df[~(is_blank_auto & has_non_blank_auto)]

    # A nivel de negocio: evitamos contar la misma combinación correo/automatismo varias veces.
    if "Automatismo" in df.columns:
        df = df.drop_duplicates(subset=["IdCorreo", "Automatismo"], keep="first")
    else:
        df = df.drop_duplicates(subset=["IdCorreo"], keep="first")

    output_path = output_dir / str(cfg_get("outputs.fichas_levantadas_xlsx", "fichas_levantadas.xlsx"))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # A nivel de negocio: exportamos el detalle completo para revisión y trazabilidad.
        df.to_excel(writer, sheet_name="Data", index=False)

        if "Segmento" in df.columns:
            # A nivel de negocio: generamos un resumen (tipo cuadro de mando) para ver cuántas fichas hay por automatismo
            # y cómo se distribuyen por segmento.
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

            # A nivel de negocio: añadimos totales por automatismo y total general para lectura rápida del volumen.
            pivot["Total"] = pivot.sum(axis=1)
            pivot.loc["Total"] = pivot.sum(axis=0)
            pivot = reordenar_pivot_blank(pivot, blank_label="(blank)", after_column="PE", keep_total_last=True)
            pivot = pivot.reset_index()
            pivot.to_excel(writer, sheet_name="Pivot", index=False)
        else:
            # A nivel de negocio: si no viene el segmento no se puede construir el reparto; dejamos aviso en el Excel.
            pd.DataFrame(
                [{"WARN": "No existe la columna 'Segmento' en fichas_levantadas.csv; no se genera Pivot."}]
            ).to_excel(writer, sheet_name="Pivot", index=False)
    logger.info("Fichas levantadas: OK -> %s (rows=%s)", output_path, df.shape[0])


def validados(fecha: str) -> None:
    #---1. Validaciones---- Enriquecer con `ia-transacciones.csv`, y generar `validaciones.xlsx` (Data + Pivot + estilos).
    logger.info("Validaciones: inicio (fecha=%s)", fecha)
    base_dir, output_dir = get_dirs(fecha)

    # A nivel de negocio: consolidamos el universo de correos a validar (una fila por correo) para evitar dobles conteos.
    df = pd.read_csv(base_dir / str(cfg_get("inputs.validaciones_csv", "validaciones.csv")))
    df = df.drop_duplicates(subset=["IdCorreo"], keep="first")

    # A nivel de negocio: incorporamos contexto de IA (asunto/pregunta/localización) para facilitar el análisis del caso.
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

    # A nivel de negocio: cruzamos por IdCorreo para enriquecer cada validación con la información disponible en IA.
    df = pd.merge(df, df_ia, how="left", left_on="IdCorreo", right_on="idLotus")
    # A nivel de negocio: marcamos casos con información incompleta que podría impedir una apertura/gestión correcta.
    df["Faltan datos?"] = df.apply(lambda x: faltan_datos(x.Automatismo, x.MailToAgent), axis=1)

    # A nivel de negocio: ordenamos para priorizar los casos más recientes dentro de cada automatismo.
    df.sort_values(by=["Automatismo", "@timestamp"], ascending=[True, False], inplace=True)
    output_path = output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx"))
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # A nivel de negocio: exportamos el detalle para poder auditar y revisar casos uno a uno.
        df.to_excel(writer, sheet_name="Data", index=False)

        df_pivot_src = df.copy()
        if "Segmento" in df_pivot_src.columns:
            df_pivot_src["Segmento"] = df_pivot_src["Segmento"].fillna("Sin Segmento")
        if "Validado" in df_pivot_src.columns:
            df_pivot_src["Validado"] = df_pivot_src["Validado"].fillna("Sin Validado")

        if all(col in df_pivot_src.columns for col in ["Automatismo", "Validado", "Segmento", "IdCorreo"]):
            # A nivel de negocio: generamos un resumen para entender, por automatismo, qué volumen está validado/no validado
            # y cómo se reparte por segmento.
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
            # A nivel de negocio: si faltan campos clave no se puede hacer el resumen; dejamos aviso en el Excel.
            pd.DataFrame(
                [{"WARN": "Faltan columnas para generar Pivot (Automatismo, Validado, Segmento, IdCorreo)."}]
            ).to_excel(writer, sheet_name="Pivot", index=False)

    # A nivel de negocio: aplicamos formato para que el informe sea fácil de leer (cabeceras, colores, etc.).
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

    # A nivel de negocio: partimos de las validaciones ya generadas y nos centramos solo en las que están "VALIDADO".
    df_validaciones = pd.read_excel(
        output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx")), sheet_name="Data"
    )
    df_validaciones = df_validaciones.drop_duplicates(subset=["IdCorreo"], keep="first")
    df_validaciones = df_validaciones[
        ["IdCorreo", "Validado", "Documento", "Automatismo", "Segmento", "MatriculaAsesor"]
    ]
    df_validaciones = df_validaciones[df_validaciones["Validado"] == "VALIDADO"]

    # A nivel de negocio: analizamos el nivel de ejecución por cada automatismo (cada tipo de proceso por separado).
    automatismos = sorted(df_validaciones["Automatismo"].dropna().unique().tolist())

    # A nivel de negocio: cargamos la evidencia de ejecución (orquestador) para comprobar si el caso validado se llegó a lanzar.
    df_orquestador_contexto = pd.read_csv(
        base_dir / str(cfg_get("inputs.orquestador_contexto_csv", "orquestador_contexto.csv"))
    )
    df_orquestador_contexto["IDU"] = df_orquestador_contexto["IDU"].apply(lambda x: str(x).split("-")[0])
    df_orquestador_contexto = df_orquestador_contexto.drop_duplicates(subset=["IDU"], keep="first")[["IDU"]]

    cruces = []

    for automatismo in automatismos:
        # A nivel de negocio: calculamos el % de casos validados que aparecen como ejecutados en orquestador.
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
        # A nivel de negocio: si no hay casos validados, no se puede medir ejecución; dejamos el informe vacío con aviso.
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="Data", index=False)
            pd.DataFrame([{"WARN": "No hay registros VALIDADO para generar ejecuciones/pivot."}]).to_excel(
                writer, sheet_name="Pivot", index=False
            )
        return

    # A nivel de negocio: consolidamos el detalle del cruce (validación vs ejecución) para revisión caso a caso.
    df_out = pd.concat(cruces, ignore_index=True)
    df_out = df_out[["IdCorreo Validacion", "Automatismo", "Segmento", "Documento", "MatriculaAsesor", "IDU contexto"]]
    # A nivel de negocio: etiquetamos cada caso como encontrado/no encontrado en orquestador (ejecutado vs no ejecutado).
    df_out["Encontrados"] = df_out["IDU contexto"].apply(lambda x: "SI" if pd.notna(x) else "NO")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # A nivel de negocio: exportamos el detalle del cruce para trazabilidad.
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

        # A nivel de negocio: generamos un resumen por automatismo, separando ejecutados vs no ejecutados y su reparto por segmento.
        pivot = df_pivot_src.pivot_table(
            index="Automatismo",
            columns=["Encontrados", "Segmento"],
            values="IdCorreo Validacion",
            aggfunc="count",
            fill_value=0,
            dropna=False,
        )
        pivot = reordenar_pivot_blank_multiindex(pivot, blank_label="(blank)", after_segment="PE")

        # A nivel de negocio: añadimos totales por automatismo y un total general para ver el volumen de un vistazo.
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
