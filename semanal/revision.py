from __future__ import annotations

import hashlib
import json
import logging
import math
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


def _load_or_init_revision_weights(weights_path: Path, automatismos: list[str], segmentos: list[str]) -> dict:
    if weights_path.exists():
        with weights_path.open("r", encoding="utf-8") as f:
            weights = json.load(f)

        changed = False
        if "default" not in weights:
            weights["default"] = 1.0
            changed = True
        if "automatismo" not in weights or not isinstance(weights["automatismo"], dict):
            weights["automatismo"] = {}
            changed = True
        if "segmento" not in weights or not isinstance(weights["segmento"], dict):
            weights["segmento"] = {}
            changed = True
        if "pair" not in weights or not isinstance(weights["pair"], dict):
            weights["pair"] = {}
            changed = True

        for a in sorted(set(automatismos)):
            if a not in weights["automatismo"]:
                weights["automatismo"][a] = 1.0
                changed = True
        for s in sorted(set(segmentos)):
            if s not in weights["segmento"]:
                weights["segmento"][s] = 1.0
                changed = True

        if changed:
            with weights_path.open("w", encoding="utf-8") as f:
                json.dump(weights, f, ensure_ascii=False, indent=2)

        return weights

    weights = {
        "default": 1.0,
        "automatismo": {a: 1.0 for a in sorted(set(automatismos))},
        "segmento": {s: 1.0 for s in sorted(set(segmentos))},
        "pair": {},
    }
    with weights_path.open("w", encoding="utf-8") as f:
        json.dump(weights, f, ensure_ascii=False, indent=2)
    return weights


def _weight_for(weights: dict, automatismo: str, segmento: str) -> float:
    default = float(weights.get("default", 1.0))
    w_a = float(weights.get("automatismo", {}).get(automatismo, 1.0))
    w_s = float(weights.get("segmento", {}).get(segmento, 1.0))
    pair_key = f"{automatismo}||{segmento}"
    w_p = float(weights.get("pair", {}).get(pair_key, 1.0))
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


def generar_validaciones_revision(fecha: str, sample_size: int = 150, min_full_stratum: int = 5) -> None:
    #---4. Revision NO VALIDADOS---- Muestreo estratificado y genera `validaciones_revision.xlsx` (Data + Resumen).
    logger.info(
        "Revision validaciones: inicio (fecha=%s, sample_size=%s, min_full_stratum=%s)",
        fecha,
        sample_size,
        min_full_stratum,
    )
    _, output_dir = get_dirs(fecha)
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

    automatismos = sorted(df_no["Automatismo"].unique().tolist())
    segmentos = sorted(df_no["Segmento"].unique().tolist())
    weights = _load_or_init_revision_weights(weights_path, automatismos, segmentos)

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

    target_n = min(int(sample_size), int(df_no.shape[0]))
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
    remaining_n = target_n - (small_total if use_small_full else 0)
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

    if use_small_full:
        sampled_parts.extend([df for df in fixed_samples if not df.empty])

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
            sampled_parts.append(df_stratum.sample(n=int(k), random_state=rs))

    df_sample = pd.concat(sampled_parts, ignore_index=True) if sampled_parts else df_no.head(0)

    resumen_df = pd.DataFrame(resumen_rows)
    resumen_totales = pd.DataFrame(
        [
            {
                "Automatismo": "Total",
                "Segmento": "",
                "Poblacion_NO_VALIDADO": int(df_no.shape[0]),
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
