from __future__ import annotations

import logging

from semanal.cli import parse_args
from semanal.config import cfg_get, load_config, resolve_repo_path
from semanal.all_report import generar_all_xlsx
from semanal.outputs import ejecuciones, fichas_levantadas, validados
from semanal.paths import get_dirs
from semanal.revision import generar_validaciones_revision
from semanal.stats import generar_stats_desde_outputs
from semanal.subtematicas import run as subtematicas_run


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        load_config(None)
        args = parse_args()
        fecha = args.fecha

        #---1. Validaciones---- Genera `validaciones.xlsx` (Data + Pivot) y colorea cabecera.
        validados(fecha)

        #---2. Fichas levantadas---- Genera `fichas_levantadas.xlsx` (Data + Pivot).
        fichas_levantadas(fecha)

        #---3. Ejecuciones---- Cruce VALIDADO vs orquestador_contexto y genera `ejecuciones.xlsx` (Data + Pivot).
        ejecuciones(fecha)

        #---4. Revision NO VALIDADOS---- Muestreo estratificado y genera `validaciones_revision.xlsx`.
        sample_size = int(cfg_get("revision.sample_size", 150))
        min_full = int(cfg_get("revision.min_full_stratum", 5))
        generar_validaciones_revision(fecha, sample_size=sample_size, min_full_stratum=min_full)

        #---5. Subtematicas---- Analisis IA/RPA y genera `mails_por_subtematica.xlsx`.
        base_dir, _ = get_dirs(fecha)
        segmento_csv = resolve_repo_path(cfg_get("paths.segmento_csv", "data/CIF-Segmento_3.csv"))
        subtematicas_run(str(base_dir), str(segmento_csv), None)

        #---6. Stats---- Copia plantilla y genera `Stats_<fecha>.xlsx` agregando hojas resumen/pivots.
        generar_stats_desde_outputs(fecha)

        #---7. All---- Cruce final (1 fila por correo) en `all.xlsx`.
        generar_all_xlsx(fecha)
    except Exception:
        logging.getLogger(__name__).exception("Fallo en workflow semanal")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
