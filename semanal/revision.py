from __future__ import annotations

import hashlib
import json
import logging
import math
import unicodedata
from pathlib import Path

import pandas as pd

from .config import cfg_get
from .paths import get_dirs, repo_root

logger = logging.getLogger(__name__)


def _stable_int_seed(seed: int, *parts: str) -> int:
    h = hashlib.md5()
    h.update(str(seed).encode("utf-8"))
    for part in parts:
        h.update(b"|")
        h.update(str(part).encode("utf-8"))
    return int.from_bytes(h.digest()[:4], "big", signed=False)


def _norm_text(value: object) -> str:
    txt = "" if value is None else str(value)
    txt = "".join(c for c in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(c))
    return txt.strip().lower()


def _load_revision_config(weights_path: Path) -> dict:
    if not weights_path.exists():
        raise FileNotFoundError(
            f"No existe {weights_path}. El JSON es el maestro y debe estar creado/actualizado antes de ejecutar el muestreo."
        )

    with weights_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Read-only: el JSON actúa de maestro (no se modifica ni se completa automáticamente).
    return cfg


def _is_blank_text_series(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    s_norm = s.apply(_norm_text)
    return s_norm.eq("") | s_norm.eq("nan")


def _weight_for(weights: dict, automatismo: str, segmento: str) -> float:
    default = float(weights.get("default", 1.0))

    auto_map = weights.get("automatismo", {}) or {}
    seg_map = weights.get("segmento", {}) or {}
    pair_map = weights.get("pair", {}) or {}

    a_norm = _norm_text(automatismo)
    s_norm = _norm_text(segmento)

    auto_norm_map = {_norm_text(k): v for k, v in auto_map.items()}
    seg_norm_map = {_norm_text(k): v for k, v in seg_map.items()}
    pair_norm_map = {_norm_text(k): v for k, v in pair_map.items()}

    w_a = float(auto_norm_map.get(a_norm, 1.0))
    w_s = float(seg_norm_map.get(s_norm, 1.0))
    pair_key_norm = _norm_text(f"{automatismo}||{segmento}")
    w_p = float(pair_norm_map.get(pair_key_norm, 1.0))

    w = default * w_a * w_s * w_p
    return max(w, 0.0)


def _allocate_samples(strata: list[dict], total_n: int) -> list[int]:
    total_n = max(int(total_n), 0)
    n_strata = len(strata)
    if n_strata == 0 or total_n == 0:
        return [0 for _ in strata]

    pops = [int(s["N"]) for s in strata]
    weights = [float(s["weight"]) for s in strata]
    eff = [p * w for p, w in zip(pops, weights)]
    eff_total = sum(eff)

    if eff_total <= 0:
        eff = pops[:]
        eff_total = sum(eff)
        if eff_total <= 0:
            return [0 for _ in strata]

    base = [0 for _ in strata]
    nonzero = [i for i, p in enumerate(pops) if p > 0]
    if total_n >= len(nonzero):
        for i in nonzero:
            base[i] = 1

    remaining = total_n - sum(base)
    if remaining <= 0:
        alloc = base[:]
    else:
        raw = [remaining * (e / eff_total) for e in eff]
        add = [int(math.floor(x)) for x in raw]
        alloc = [b + a for b, a in zip(base, add)]

        remainder = remaining - sum(add)
        frac = [r - math.floor(r) for r in raw]
        order = sorted(range(n_strata), key=lambda i: frac[i], reverse=True)
        for i in order:
            if remainder <= 0:
                break
            alloc[i] += 1
            remainder -= 1

    alloc = [min(a, p) for a, p in zip(alloc, pops)]
    deficit = total_n - sum(alloc)
    if deficit > 0:
        capacities = [p - a for p, a in zip(pops, alloc)]
        order = sorted(range(n_strata), key=lambda i: eff[i], reverse=True)
        for i in order:
            if deficit <= 0:
                break
            if capacities[i] <= 0:
                continue
            take = min(capacities[i], deficit)
            alloc[i] += take
            deficit -= take

    return alloc


def _sample_distributed_by_day(df_stratum: pd.DataFrame, n: int, random_state: int, seed_parts: list[str]) -> pd.DataFrame:
    if n <= 0 or df_stratum.empty:
        return df_stratum.head(0)

    if "@timestamp" not in df_stratum.columns:
        return df_stratum.sample(n=int(n), random_state=int(random_state))

    ts = pd.to_datetime(df_stratum["@timestamp"], errors="coerce", utc=True)
    if ts.isna().all():
        return df_stratum.sample(n=int(n), random_state=int(random_state))

    tmp = df_stratum.copy()
    tmp["__day__"] = ts.dt.date.astype(str)

    day_counts = tmp["__day__"].value_counts().sort_index()
    day_strata = [{"N": int(day_counts[d]), "weight": 1.0} for d in day_counts.index.tolist()]
    alloc = _allocate_samples(day_strata, int(n))

    parts = []
    for day, k in zip(day_counts.index.tolist(), alloc):
        if k <= 0:
            continue
        df_day = tmp[tmp["__day__"] == day]
        rs = _stable_int_seed(int(random_state), *seed_parts, str(day))
        parts.append(df_day.sample(n=int(k), random_state=int(rs)))

    out = pd.concat(parts, ignore_index=True) if parts else tmp.head(0)
    return out.drop(columns=["__day__"], errors="ignore")


def _align_like(df: pd.DataFrame, columns: list[str], fill_value: str = "-") -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = fill_value
    out = out[columns].copy()
    return out.fillna(fill_value)


def generar_validaciones_revision(fecha: str, sample_size: int = 150, min_full_stratum: int = 5) -> None:
    #---4. Revision NO VALIDADOS---- Muestreo estratificado y genera `validaciones_revision.xlsx` (Data + Resumen).
    logger.info(
        "Revision validaciones: inicio (fecha=%s, sample_size=%s, min_full_stratum=%s)",
        fecha,
        sample_size,
        min_full_stratum,
    )
    base_dir, output_dir = get_dirs(fecha)
    validaciones_path = output_dir / str(cfg_get("outputs.validaciones_xlsx", "validaciones.xlsx"))
    revision_path = output_dir / str(cfg_get("outputs.revision_xlsx", "validaciones_revision.xlsx"))

    weights_path = repo_root() / str(cfg_get("paths.revision_weights", "validaciones_revision_pesos.json"))
    legacy_weights_paths = [
        output_dir / "validaciones_revision_pesos.json",
        repo_root() / "scriptsOld" / "validaciones_revision_pesos.json",
    ]
    if not weights_path.exists():
        for legacy_path in legacy_weights_paths:
            if legacy_path.exists():
                weights_path.write_text(legacy_path.read_text(encoding="utf-8"), encoding="utf-8")
                break

    df = pd.read_excel(validaciones_path, sheet_name="Data")
    if "Validado" not in df.columns:
        with pd.ExcelWriter(revision_path, engine="openpyxl") as writer:
            pd.DataFrame([{"WARN": f"No existe la columna 'Validado' en {validaciones_path.name}"}]).to_excel(
                writer, sheet_name="Resumen", index=False
            )
        return

    # Pool principal de revisión: NO VALIDADOS (`Validado != "VALIDADO"`).
    df_no = df[df["Validado"].fillna("") != "VALIDADO"].copy()
    if df_no.empty:
        with pd.ExcelWriter(revision_path, engine="openpyxl") as writer:
            pd.DataFrame([{"WARN": "No hay NO VALIDADOS para muestrear."}]).to_excel(
                writer, sheet_name="Resumen", index=False
            )
        return

    if "Automatismo" not in df_no.columns:
        df_no["Automatismo"] = "(blank)"
    if "Segmento" not in df_no.columns:
        df_no["Segmento"] = "(blank)"
    if "IdCorreo" not in df_no.columns:
        df_no["IdCorreo"] = "(blank)"

    df_no["Automatismo"] = df_no["Automatismo"].fillna("(blank)").astype(str).replace({"": "(blank)"})
    df_no["Segmento"] = df_no["Segmento"].fillna("(blank)").astype(str).replace({"": "(blank)"})

    weights = _load_revision_config(weights_path)

    required_fields = weights.get("required_fields") or ["Question", "MailToAgent"]
    required_fields = [str(c) for c in required_fields]
    for c in required_fields:
        if c not in df_no.columns:
            df_no[c] = ""

    # Antes de muestrear: excluir casos que no tienen la información mínima para revisión.
    missing_mask = pd.Series(False, index=df_no.index)
    for c in required_fields:
        missing_mask = missing_mask | _is_blank_text_series(df_no[c])
    df_no = df_no[~missing_mask].copy()

    # Cupos fijos: correos que se quieren incluir sí o sí (dentro de los 150) independientemente del estrato.
    fixed_cfg = weights.get("fixed", {}) or {}
    fixed_accion_n = int(fixed_cfg.get("Accion no requerida", 0) or 0)

    fixed_samples_df = df_no.head(0)
    accion_population = 0
    if fixed_accion_n > 0:
        ia_transacciones_csv = base_dir / str(cfg_get("inputs.ia_transacciones_csv", "ia-transacciones.csv"))
        if ia_transacciones_csv.exists():
            try:
                df_ia = pd.read_csv(ia_transacciones_csv, sep=";", dtype=str, keep_default_na=False)
                if "idLotus" in df_ia.columns:
                    df_ia = df_ia.rename(columns={"idLotus": "IdCorreo"})
                if "@timestamp" not in df_ia.columns and "IA_timestamp" in df_ia.columns:
                    df_ia = df_ia.rename(columns={"IA_timestamp": "@timestamp"})

                loc_norm = df_ia.get("Location", "").astype(str).apply(_norm_text) if "Location" in df_ia.columns else None
                sub_norm = (
                    df_ia.get("Sublocation", "").astype(str).apply(_norm_text) if "Sublocation" in df_ia.columns else None
                )
                is_accion = False
                if loc_norm is not None:
                    is_accion = loc_norm.eq("accion no requerida")
                if sub_norm is not None:
                    is_accion = is_accion | sub_norm.eq("accion no requerida") if hasattr(is_accion, "__or__") else sub_norm.eq("accion no requerida")

                df_accion = df_ia[is_accion].copy() if hasattr(is_accion, "__len__") else df_ia.head(0)
                if not df_accion.empty:
                    accion_population = int(df_accion["IdCorreo"].astype(str).nunique()) if "IdCorreo" in df_accion.columns else int(df_accion.shape[0])
                    df_accion["Automatismo"] = "Accion no requerida"
                    # Segmento no existe en IA; se rellena con '-' (se intenta enriquecer luego desde validaciones si aplica).
                    df_accion["Segmento"] = "-"

                    # Normalización para "Accion no requerida":
                    # - `idLotus` debe contener el mismo identificador que `IdCorreo`.
                    # - El timestamp debe venir de IA: `IA_timestamp` (y se copia a `@timestamp` para esta casuística).
                    if "IdCorreo" in df_accion.columns and "idLotus" in df.columns:
                        df_accion["idLotus"] = df_accion["IdCorreo"]
                    if "@timestamp" in df_accion.columns:
                        if "IA_timestamp" in df.columns:
                            df_accion["IA_timestamp"] = df_accion["@timestamp"]
                        if "@timestamp" in df.columns and "IA_timestamp" in df_accion.columns:
                            df_accion["@timestamp"] = df_accion["IA_timestamp"]

                    # Enriquecer con campos que sí vienen de validaciones (si existen) para facilitar la revisión.
                    enrich_cols = [c for c in ["IdCorreo", "Documento", "Segmento", "MatriculaAsesor"] if c in df.columns]
                    if enrich_cols and "IdCorreo" in df_accion.columns:
                        df_accion = pd.merge(
                            df_accion,
                            df[enrich_cols].drop_duplicates(subset=["IdCorreo"], keep="first"),
                            how="left",
                            on="IdCorreo",
                            suffixes=("", "_val"),
                        )
                        if "Segmento_val" in df_accion.columns:
                            df_accion["Segmento"] = df_accion["Segmento_val"].where(
                                df_accion["Segmento_val"].astype(str).str.strip().str.len() > 0, df_accion["Segmento"]
                            )
                            df_accion = df_accion.drop(columns=["Segmento_val"], errors="ignore")

                    for c in required_fields:
                        if c not in df_accion.columns:
                            df_accion[c] = ""

                    missing_mask = pd.Series(False, index=df_accion.index)
                    for c in required_fields:
                        missing_mask = missing_mask | _is_blank_text_series(df_accion[c])
                    df_accion = df_accion[~missing_mask].copy()

                    fixed_take = min(int(fixed_accion_n), int(sample_size))
                    fixed_rs = _stable_int_seed(42, fecha, "Accion no requerida", "fixed")
                    fixed_samples_df = _sample_distributed_by_day(
                        df_accion,
                        fixed_take,
                        fixed_rs,
                        [fecha, "Accion no requerida", "fixed"],
                    )
            except Exception as exc:
                logger.warning("Revision validaciones: no se pudo leer/filtrar %s (%s)", ia_transacciones_csv, exc)

    fixed_ids = set()
    if not fixed_samples_df.empty and "IdCorreo" in fixed_samples_df.columns:
        fixed_ids = set(fixed_samples_df["IdCorreo"].astype(str).tolist())
        df_no = df_no[~df_no["IdCorreo"].astype(str).isin(fixed_ids)]

    # Estratos (solo NO VALIDADOS; el cupo fijo de "Accion no requerida" se reporta aparte en el resumen).
    strata_df = (
        df_no.groupby(["Automatismo", "Segmento"], dropna=False)
        .size()
        .reset_index(name="N")
        .sort_values(["Automatismo", "Segmento"])
    )

    strata = []
    for row in strata_df.itertuples(index=False):
        w = _weight_for(weights, row.Automatismo, row.Segmento)
        strata.append({"Automatismo": row.Automatismo, "Segmento": row.Segmento, "N": int(row.N), "weight": w})

    target_n = min(int(sample_size), int(df_no.shape[0]) + int(len(fixed_samples_df)))
    fixed_n = min(int(len(fixed_samples_df)), int(target_n))
    remaining_budget = int(target_n) - int(fixed_n)
    min_full_stratum = max(int(min_full_stratum), 0)

    small_strata = [s for s in strata if s["N"] > 0 and s["N"] <= min_full_stratum]
    small_total = sum(int(s["N"]) for s in small_strata)
    use_small_full = small_total > 0 and small_total <= target_n

    fixed_samples = []
    fixed_keys = set()
    if use_small_full:
        for s in small_strata:
            a = s["Automatismo"]
            seg = s["Segmento"]
            fixed_keys.add((a, seg))
            fixed_samples.append(df_no[(df_no["Automatismo"] == a) & (df_no["Segmento"] == seg)])

    remaining_strata = [s for s in strata if (s["Automatismo"], s["Segmento"]) not in fixed_keys]
    remaining_n = remaining_budget - (small_total if use_small_full else 0)
    alloc = _allocate_samples(remaining_strata, remaining_n) if remaining_strata and remaining_n > 0 else []

    sampled_parts = []
    resumen_rows = []

    for s in strata:
        a = s["Automatismo"]
        seg = s["Segmento"]
        pop = s["N"]
        w = s["weight"]

        k = pop if (use_small_full and (a, seg) in fixed_keys) else 0
        resumen_rows.append(
            {
                "Automatismo": a,
                "Segmento": seg,
                "Poblacion_NO_VALIDADO": pop,
                "Peso": w,
                "Muestra": int(k),
            }
        )

    if fixed_accion_n > 0:
        resumen_rows.insert(
            0,
            {
                "Automatismo": "Accion no requerida",
                "Segmento": "-",
                "Poblacion_NO_VALIDADO": int(accion_population),
                "Peso": float(weights.get("automatismo", {}).get("Accion no requerida", 1.0)),
                "Muestra": int(fixed_n),
            },
        )

    if use_small_full:
        sampled_parts.extend([df for df in fixed_samples if not df.empty])

    # Añadir los cupos fijos al muestreo (incluidos dentro del presupuesto total).
    if fixed_n > 0 and not fixed_samples_df.empty:
        out_cols = df_no.columns.tolist()
        sampled_parts.insert(0, _align_like(fixed_samples_df, out_cols, fill_value="-"))

    if remaining_strata and alloc:
        alloc_map = {(s["Automatismo"], s["Segmento"]): int(k) for s, k in zip(remaining_strata, alloc)}
        for row in resumen_rows:
            key = (row["Automatismo"], row["Segmento"])
            if key in alloc_map:
                row["Muestra"] = alloc_map[key]

        for s, k in zip(remaining_strata, alloc):
            if k <= 0:
                continue

            a = s["Automatismo"]
            seg = s["Segmento"]
            df_stratum = df_no[(df_no["Automatismo"] == a) & (df_no["Segmento"] == seg)]
            rs = _stable_int_seed(42, fecha, a, seg)
            sampled_parts.append(_sample_distributed_by_day(df_stratum, int(k), rs, [fecha, a, seg]))

    df_sample = pd.concat(sampled_parts, ignore_index=True) if sampled_parts else df_no.head(0)

    # Post-condición: garantizar mínimos; si por cualquier motivo entró alguno sin campos requeridos, se excluye.
    for c in required_fields:
        if c not in df_sample.columns:
            df_sample[c] = ""
    missing_mask = pd.Series(False, index=df_sample.index)
    for c in required_fields:
        missing_mask = missing_mask | _is_blank_text_series(df_sample[c])
    df_sample = df_sample[~missing_mask].copy()

    # Evitar "nan" visual en el Excel: el resto de campos vacíos se pintan con "-".
    df_sample = df_sample.fillna("-")

    resumen_df = pd.DataFrame(resumen_rows)
    resumen_totales = pd.DataFrame(
        [
            {
                "Automatismo": "Total",
                "Segmento": "",
                "Poblacion_NO_VALIDADO": int(resumen_df["Poblacion_NO_VALIDADO"].sum()) if not resumen_df.empty else 0,
                "Peso": "",
                "Muestra": int(df_sample.shape[0]),
            }
        ]
    )
    resumen_df = pd.concat([resumen_df, resumen_totales], ignore_index=True)

    with pd.ExcelWriter(revision_path, engine="openpyxl") as writer:
        df_sample.to_excel(writer, sheet_name="Data", index=False)
        resumen_df.to_excel(writer, sheet_name="Resumen", index=False)

    logger.info(
        "Revision validaciones: OK -> %s (poblacion_no_validado=%s, muestra=%s)",
        revision_path,
        df_no.shape[0],
        df_sample.shape[0],
    )
