#!/usr/bin/env python3
"""Production runner for the Latent Insider Information Shocks project.

The runner is intentionally conservative around WRDS:
- schema and smoke first;
- shard by year;
- write manifests next to every durable output;
- stop on authentication/connection timeouts;
- never print credentials or API keys.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import contextlib
import dataclasses
import datetime as dt
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import traceback
import urllib.request
import zipfile

import numpy as np
import pandas as pd


DEFAULT_CONFIG = {
    "sample": {
        "start_date": "2004-01-01",
        "end_date": "2025-12-31",
        "holdout_start": "2026-01-01",
        "holdout_end": "2026-05-31",
    },
    "wrds": {
        "max_workers": 3,
        "connect_timeout_seconds": 180,
        "shard_retry_count": 1,
        "libraries": {
            "insiders": "tr_insiders",
            "crsp": "crsp",
            "comp": "comp",
            "taq_candidates": ["taqmsec", "taqms", "taq", "taqm"],
        },
    },
    "models": {
        "temporal_split": {
            "train_end": "2016-12-31",
            "validation_start": "2017-01-01",
            "validation_end": "2020-12-31",
            "oos_start": "2021-01-01",
            "oos_end": "2025-12-31",
            "final_holdout_start": "2026-01-01",
        }
    },
}


@dataclasses.dataclass(frozen=True)
class Paths:
    root: Path
    logs: Path
    manifests: Path
    raw: Path
    processed: Path
    intermediate: Path
    tables: Path
    figures_static: Path
    figures_html: Path
    models: Path


def project_paths(root: Path) -> Paths:
    artifacts = root / "artifacts"
    return Paths(
        root=root,
        logs=root / "logs",
        manifests=root / "manifests",
        raw=artifacts / "raw",
        processed=artifacts / "processed",
        intermediate=artifacts / "intermediate",
        tables=artifacts / "tables",
        figures_static=artifacts / "figures_static",
        figures_html=artifacts / "figures_html",
        models=artifacts / "models",
    )


def ensure_dirs(paths: Paths) -> None:
    for value in dataclasses.asdict(paths).values():
        Path(value).mkdir(parents=True, exist_ok=True)
    for sub in [
        paths.raw / "wrds",
        paths.raw / "wrds" / "insiders",
        paths.raw / "wrds" / "insiders_table2",
        paths.raw / "wrds" / "insider_header",
        paths.raw / "wrds" / "insider_support",
        paths.raw / "wrds" / "crsp_msf",
        paths.raw / "wrds" / "crsp_dsf",
        paths.raw / "wrds" / "crsp_names",
        paths.raw / "wrds" / "comp_funda",
        paths.raw / "wrds" / "comp_fundq",
        paths.raw / "wrds" / "ccm",
        paths.raw / "wrds" / "taq",
        paths.raw / "public",
        paths.processed / "features",
        paths.processed / "panels",
        paths.processed / "scores",
        paths.processed / "robustness",
        paths.tables / "model_cards",
    ]:
        sub.mkdir(parents=True, exist_ok=True)


def setup_logging(paths: Paths) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = paths.logs / f"liis_pipeline_{stamp}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)],
    )
    logging.info("log_path=%s", log_path)
    return log_path


def deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config(root: Path) -> dict:
    cfg_path = root / "configs" / "pipeline.yml"
    if not cfg_path.exists():
        return DEFAULT_CONFIG
    try:
        import yaml

        with cfg_path.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        return deep_merge(DEFAULT_CONFIG, loaded)
    except Exception as exc:  # pragma: no cover - defensive fallback
        print(f"WARNING: failed to parse {cfg_path}: {exc}; using defaults")
        return DEFAULT_CONFIG


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True, default=str)
    tmp.replace(path)


def manifest(paths: Paths, name: str, payload: dict) -> Path:
    payload = {
        "name": name,
        "created_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        **payload,
    }
    path = paths.manifests / f"{name}.json"
    write_json(path, payload)
    logging.info("manifest=%s", path)
    return path


def years_between(start_date: str, end_date: str) -> list[int]:
    start = pd.Timestamp(start_date).year
    end = pd.Timestamp(end_date).year
    return list(range(start, end + 1))


def safe_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name or ""):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def safe_table(lib: str, table: str) -> str:
    return f"{safe_ident(lib)}.{safe_ident(table)}"


def import_wrds():
    import wrds  # type: ignore

    return wrds


def wrds_smoke_or_stop(paths: Paths, timeout_seconds: int) -> None:
    logging.info("starting WRDS smoke test with timeout=%s", timeout_seconds)
    code = r"""
import json
try:
    import wrds
    db = wrds.Connection()
    result = db.raw_sql("select 1 as ok")
    try:
        db.close()
    except Exception:
        pass
    print(json.dumps({"ok": True, "rows": int(len(result))}))
except Exception as exc:
    print(json.dumps({"ok": False, "error_type": type(exc).__name__, "error": str(exc)[:500]}))
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        payload = {
            "status": "blocked",
            "reason": "WRDS connection timed out; possible MFA/push approval needed",
            "timeout_seconds": timeout_seconds,
        }
        manifest(paths, "wrds_smoke_blocked", payload)
        raise SystemExit("WRDS connection timed out. Stop and wait for user/MFA.")

    stdout = completed.stdout.strip().splitlines()
    if not stdout:
        payload = {
            "status": "failed",
            "reason": "WRDS smoke produced no stdout",
            "returncode": completed.returncode,
            "stderr_tail": completed.stderr[-1000:],
        }
        manifest(paths, "wrds_smoke_failed", payload)
        raise SystemExit("WRDS smoke failed without result")
    try:
        result = json.loads(stdout[-1])
    except json.JSONDecodeError:
        payload = {
            "status": "failed",
            "reason": "WRDS smoke stdout was not JSON",
            "returncode": completed.returncode,
            "stdout_tail": completed.stdout[-1000:],
            "stderr_tail": completed.stderr[-1000:],
        }
        manifest(paths, "wrds_smoke_failed", payload)
        raise SystemExit("WRDS smoke failed with non-JSON result")
    manifest(paths, "wrds_smoke", {"status": "ok" if result["ok"] else "failed", **result})
    if not result["ok"]:
        raise SystemExit(f"WRDS smoke failed: {result.get('error_type')}")


def wrds_connection():
    wrds = import_wrds()
    return wrds.Connection()


def describe_table(db, library: str, table: str) -> pd.DataFrame:
    try:
        return db.describe_table(library=library, table=table)
    except TypeError:
        return db.describe_table(library, table)


def extract_column_names(desc: pd.DataFrame) -> list[str]:
    if desc is None or desc.empty:
        return []
    for candidate in ["name", "column_name", "varname", "field", "Column"]:
        if candidate in desc.columns:
            return [str(x).lower() for x in desc[candidate].dropna().tolist()]
    return [str(x).lower() for x in desc.iloc[:, 0].dropna().tolist()]


def audit_schema(paths: Paths, cfg: dict) -> dict:
    logging.info("auditing WRDS schema")
    db = wrds_connection()
    libraries = sorted([str(x).lower() for x in db.list_libraries()])
    libs_cfg = cfg["wrds"]["libraries"]
    requested = [
        libs_cfg["insiders"],
        libs_cfg["crsp"],
        libs_cfg["comp"],
        *libs_cfg.get("taq_candidates", []),
    ]
    schema: dict[str, object] = {
        "library_count": len(libraries),
        "requested_libraries": requested,
        "available_requested_libraries": [lib for lib in requested if lib in libraries],
        "libraries_visible_sample": libraries[:40],
        "tables": {},
        "columns": {},
    }

    for lib in requested:
        lib = str(lib).lower()
        if lib not in libraries:
            logging.warning("library missing or not entitled: %s", lib)
            continue
        try:
            tables = sorted([str(x).lower() for x in db.list_tables(lib)])
        except Exception as exc:
            logging.exception("failed listing tables for %s", lib)
            schema["tables"][lib] = {"error": type(exc).__name__, "message": str(exc)[:500]}
            continue
        schema["tables"][lib] = tables
        schema["columns"][lib] = {}
        interesting = tables
        if lib in {"crsp", "comp"}:
            keep = {
                "msf",
                "dsf",
                "msenames",
                "dsedelist",
                "ccmxpf_lnkhist",
                "funda",
                "fundq",
            }
            interesting = [tbl for tbl in tables if tbl in keep]
        elif lib.startswith("taq"):
            interesting = tables[:20]
        for tbl in interesting[:200]:
            try:
                desc = describe_table(db, lib, tbl)
                schema["columns"][lib][tbl] = extract_column_names(desc)
            except Exception as exc:
                schema["columns"][lib][tbl] = {
                    "error": type(exc).__name__,
                    "message": str(exc)[:300],
                }

    with contextlib.suppress(Exception):
        db.close()

    write_json(paths.intermediate / "schema_map_runtime.json", schema)
    manifest(
        paths,
        "schema_audit",
        {
            "status": "ok",
            "library_count": schema["library_count"],
            "available_requested_libraries": schema["available_requested_libraries"],
            "schema_path": str(paths.intermediate / "schema_map_runtime.json"),
        },
    )
    return schema


def load_schema(paths: Paths) -> dict:
    path = paths.intermediate / "schema_map_runtime.json"
    if not path.exists():
        raise FileNotFoundError(f"schema audit not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def table_columns(schema: dict, lib: str, tbl: str) -> list[str]:
    obj = schema.get("columns", {}).get(lib, {}).get(tbl, [])
    return obj if isinstance(obj, list) else []


def first_existing_table(schema: dict, lib: str, candidates: list[str]) -> str | None:
    tables = set(schema.get("tables", {}).get(lib, []))
    for tbl in candidates:
        if tbl in tables:
            return tbl
    return None


def choose_date_column(columns: list[str], candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    for col in columns:
        if "date" in col.lower():
            return col
    return None


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def fetch_sql_to_parquet(
    sql: str,
    out_path: Path,
    date_cols: list[str] | None = None,
    retries: int = 1,
) -> dict:
    if out_path.exists() and out_path.stat().st_size > 0:
        return {"status": "skipped_existing", "path": str(out_path)}
    last_error: str | None = None
    for attempt in range(retries + 1):
        try:
            db = wrds_connection()
            started = time.time()
            df = db.raw_sql(sql, date_cols=date_cols or [])
            elapsed = time.time() - started
            with contextlib.suppress(Exception):
                db.close()
            write_parquet_atomic(df, out_path)
            return {
                "status": "ok",
                "path": str(out_path),
                "rows": int(len(df)),
                "columns": list(map(str, df.columns)),
                "elapsed_seconds": round(elapsed, 2),
            }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:700]}"
            logging.warning("query failed attempt=%s out=%s error=%s", attempt, out_path, last_error)
            time.sleep(min(30, 3 + attempt * 5))
    return {"status": "failed", "path": str(out_path), "error": last_error}


def bounded_parallel(jobs: list[tuple], max_workers: int) -> list[dict]:
    results: list[dict] = []
    if not jobs:
        return results
    with futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(fetch_sql_to_parquet, *job) for job in jobs]
        for fut in futures.as_completed(futs):
            result = fut.result()
            logging.info("extract_result=%s", result)
            results.append(result)
    return results


def extract_crsp_monthly(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    lib = cfg["wrds"]["libraries"]["crsp"]
    tbl = "msf"
    columns = table_columns(schema, lib, tbl)
    if tbl not in set(schema.get("tables", {}).get(lib, [])):
        return [{"status": "blocked", "reason": "crsp.msf missing"}]
    wanted = [c for c in ["permno", "date", "ret", "retx", "prc", "shrout", "vol", "cfacpr", "cfacshr"] if c in columns]
    years = years_between(cfg["sample"]["start_date"], cfg["sample"]["holdout_end"])
    jobs = []
    for year in years:
        sql = (
            f"select {', '.join(wanted)} from {safe_table(lib, tbl)} "
            f"where date between '{year}-01-01' and '{year}-12-31'"
        )
        jobs.append((sql, paths.raw / "wrds" / "crsp_msf" / f"crsp_msf_{year}.parquet", ["date"], cfg["wrds"]["shard_retry_count"]))
    return bounded_parallel(jobs, int(cfg["wrds"]["max_workers"]))


def extract_crsp_daily(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    lib = cfg["wrds"]["libraries"]["crsp"]
    tbl = "dsf"
    columns = table_columns(schema, lib, tbl)
    if tbl not in set(schema.get("tables", {}).get(lib, [])):
        return [{"status": "blocked", "reason": "crsp.dsf missing"}]
    wanted = [c for c in ["permno", "date", "ret", "retx", "prc", "shrout", "vol", "cfacpr", "cfacshr"] if c in columns]
    years = years_between(cfg["sample"]["start_date"], cfg["sample"]["holdout_end"])
    jobs = []
    for year in years:
        sql = (
            f"select {', '.join(wanted)} from {safe_table(lib, tbl)} "
            f"where date between '{year}-01-01' and '{year}-12-31'"
        )
        jobs.append((sql, paths.raw / "wrds" / "crsp_dsf" / f"crsp_dsf_{year}.parquet", ["date"], cfg["wrds"]["shard_retry_count"]))
    return bounded_parallel(jobs, int(cfg["wrds"]["max_workers"]))


def extract_crsp_names_and_ccm(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    lib = cfg["wrds"]["libraries"]["crsp"]
    results = []
    if "msenames" in set(schema.get("tables", {}).get(lib, [])):
        cols = table_columns(schema, lib, "msenames")
        wanted = [
            c
            for c in ["permno", "permco", "namedt", "nameendt", "ticker", "comnam", "cusip", "ncusip", "shrcd", "exchcd", "siccd"]
            if c in cols
        ]
        sql = (
            f"select {', '.join(wanted)} from {safe_table(lib, 'msenames')} "
            f"where namedt <= '{cfg['sample']['holdout_end']}' and "
            f"(nameendt is null or nameendt >= '{cfg['sample']['start_date']}')"
        )
        results.append(fetch_sql_to_parquet(sql, paths.raw / "wrds" / "crsp_names" / "msenames.parquet", ["namedt", "nameendt"]))
    else:
        results.append({"status": "blocked", "reason": "crsp.msenames missing"})

    if "ccmxpf_lnkhist" in set(schema.get("tables", {}).get(lib, [])):
        cols = table_columns(schema, lib, "ccmxpf_lnkhist")
        wanted = [c for c in ["gvkey", "lpermno", "lpermco", "linkdt", "linkenddt", "linktype", "linkprim", "liid"] if c in cols]
        sql = (
            f"select {', '.join(wanted)} from {safe_table(lib, 'ccmxpf_lnkhist')} "
            f"where linkdt <= '{cfg['sample']['holdout_end']}' and "
            f"(linkenddt is null or linkenddt >= '{cfg['sample']['start_date']}')"
        )
        results.append(fetch_sql_to_parquet(sql, paths.raw / "wrds" / "ccm" / "ccmxpf_lnkhist.parquet", ["linkdt", "linkenddt"]))
    else:
        results.append({"status": "blocked", "reason": "crsp.ccmxpf_lnkhist missing"})
    return results


def extract_compustat(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    lib = cfg["wrds"]["libraries"]["comp"]
    results: list[dict] = []
    specs = {
        "funda": {
            "out_dir": paths.raw / "wrds" / "comp_funda",
            "wanted": [
                "gvkey",
                "datadate",
                "fyear",
                "at",
                "ceq",
                "seq",
                "txditc",
                "pstkrv",
                "pstkl",
                "pstk",
                "sale",
                "cogs",
                "xsga",
                "oiadp",
                "ni",
                "ib",
                "capx",
                "che",
                "dltt",
                "dlc",
                "xint",
            ],
        },
        "fundq": {
            "out_dir": paths.raw / "wrds" / "comp_fundq",
            "wanted": [
                "gvkey",
                "datadate",
                "fyearq",
                "fqtr",
                "saleq",
                "niq",
                "ibq",
                "cheq",
                "dlttq",
                "dlcq",
                "cshoq",
                "prccq",
            ],
        },
    }
    jobs = []
    for tbl, spec in specs.items():
        if tbl not in set(schema.get("tables", {}).get(lib, [])):
            results.append({"status": "blocked", "reason": f"{lib}.{tbl} missing"})
            continue
        cols = table_columns(schema, lib, tbl)
        wanted = [c for c in spec["wanted"] if c in cols]
        extra_filters = ""
        for c, val in [("indfmt", "INDL"), ("datafmt", "STD"), ("popsrc", "D"), ("consol", "C")]:
            if c in cols:
                extra_filters += f" and {c} = '{val}'"
        for year in years_between(cfg["sample"]["start_date"], cfg["sample"]["holdout_end"]):
            sql = (
                f"select {', '.join(wanted)} from {safe_table(lib, tbl)} "
                f"where datadate between '{year}-01-01' and '{year}-12-31'"
                f"{extra_filters}"
            )
            jobs.append((sql, spec["out_dir"] / f"{tbl}_{year}.parquet", ["datadate"], cfg["wrds"]["shard_retry_count"]))
    results.extend(bounded_parallel(jobs, int(cfg["wrds"]["max_workers"])))
    return results


def choose_insider_table(schema: dict, lib: str) -> tuple[str | None, str | None, list[str]]:
    tables = schema.get("tables", {}).get(lib, [])
    if not isinstance(tables, list) or not tables:
        return None, None, []
    best: tuple[int, str, str | None, list[str]] = (-1, "", None, [])
    important = [
        "filing_date",
        "fdate",
        "filedate",
        "transaction_date",
        "trandate",
        "transaction_code",
        "trancode",
        "issuer_cik",
        "cik",
        "cusip",
        "ticker",
    ]
    for tbl in tables:
        cols = table_columns(schema, lib, tbl)
        if not cols:
            continue
        score = sum(1 for c in important if c in cols)
        date_col = choose_date_column(cols, ["filing_date", "fdate", "filedate", "rdate", "transaction_date", "trandate"])
        if score > best[0] and date_col:
            best = (score, tbl, date_col, cols)
    if best[0] < 1:
        return None, None, []
    return best[1], best[2], best[3]


def extract_insiders(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    lib = cfg["wrds"]["libraries"]["insiders"]
    tbl, date_col, cols = choose_insider_table(schema, lib)
    if not tbl or not date_col:
        return [{"status": "blocked", "reason": "No usable Thomson Reuters Insiders table/date column discovered"}]
    logging.info("insider_table=%s.%s date_col=%s", lib, tbl, date_col)
    selected_cols = cols
    years = years_between(cfg["sample"]["start_date"], cfg["sample"]["holdout_end"])
    jobs = []
    for year in years:
        sql = (
            f"select {', '.join(map(safe_ident, selected_cols))} from {safe_table(lib, tbl)} "
            f"where {safe_ident(date_col)} between '{year}-01-01' and '{year}-12-31'"
        )
        jobs.append((sql, paths.raw / "wrds" / "insiders" / f"{tbl}_{year}.parquet", [date_col], cfg["wrds"]["shard_retry_count"]))
    results = bounded_parallel(jobs, max(1, min(2, int(cfg["wrds"]["max_workers"]))))
    manifest(paths, "insider_table_choice", {"library": lib, "table": tbl, "date_col": date_col, "columns": selected_cols})
    return results


def extract_insider_support_tables(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    """Extract proposal-critical insider support tables.

    `table1` is the non-derivative transaction spine. These support tables add
    derivative companion activity, 10b5-1 flags, amendments, filing headers, and
    company metadata. Existing shards are skipped, so reruns are cache-safe.
    """

    lib = cfg["wrds"]["libraries"]["insiders"]
    available = set(schema.get("tables", {}).get(lib, []))
    results: list[dict] = []
    years = years_between(cfg["sample"]["start_date"], cfg["sample"]["holdout_end"])

    yearly_specs = {
        "table2": {
            "date_candidates": ["fdate", "trandate", "cdate"],
            "out_dir": paths.raw / "wrds" / "insiders_table2",
            "date_cols": ["fdate", "cdate", "trandate", "xdate", "tdate", "secdate", "sigdate", "maintdate"],
        },
        "header": {
            "date_candidates": ["fdate", "cdate", "secdate"],
            "out_dir": paths.raw / "wrds" / "insider_header",
            "date_cols": ["fdate", "cdate", "secdate", "sigdate", "maintdate"],
        },
        "rule10b5": {
            "date_candidates": ["cdate", "maintdate"],
            "out_dir": paths.raw / "wrds" / "insider_support",
            "date_cols": ["cdate", "maintdate"],
        },
        "amend": {
            "date_candidates": ["effdate", "createdate", "maintdate"],
            "out_dir": paths.raw / "wrds" / "insider_support",
            "date_cols": ["effdate", "createdate", "maintdate"],
        },
    }
    jobs = []
    for tbl, spec in yearly_specs.items():
        if tbl not in available:
            results.append({"status": "blocked", "table": tbl, "reason": "table missing"})
            continue
        cols = table_columns(schema, lib, tbl)
        date_col = choose_date_column(cols, spec["date_candidates"])
        if not date_col:
            results.append({"status": "blocked", "table": tbl, "reason": "date column missing"})
            continue
        for year in years:
            sql = (
                f"select {', '.join(map(safe_ident, cols))} from {safe_table(lib, tbl)} "
                f"where {safe_ident(date_col)} between '{year}-01-01' and '{year}-12-31'"
            )
            jobs.append(
                (
                    sql,
                    spec["out_dir"] / f"{tbl}_{year}.parquet",
                    [c for c in spec["date_cols"] if c in cols],
                    cfg["wrds"]["shard_retry_count"],
                )
            )

    # Company metadata is small and not naturally year-sharded.
    if "company" in available:
        cols = table_columns(schema, lib, "company")
        sql = f"select {', '.join(map(safe_ident, cols))} from {safe_table(lib, 'company')}"
        results.append(fetch_sql_to_parquet(sql, paths.raw / "wrds" / "insider_support" / "company.parquet", retries=0))
    else:
        results.append({"status": "blocked", "table": "company", "reason": "table missing"})

    results.extend(bounded_parallel(jobs, max(1, min(2, int(cfg["wrds"]["max_workers"])))))
    return results


def extract_taq_calibration(paths: Paths, cfg: dict, schema: dict) -> list[dict]:
    taq_libs = [lib for lib in cfg["wrds"]["libraries"].get("taq_candidates", []) if lib in schema.get("tables", {})]
    if not taq_libs:
        return [{"status": "blocked", "reason": "No TAQ candidate libraries visible"}]
    # Conservative appendix attempt: metadata/smoke only first, plus month-end table samples
    # where TAQ table names expose dates. This avoids uncontrolled full TAQ pulls.
    results: list[dict] = []
    for lib in taq_libs:
        tables = schema.get("tables", {}).get(lib, [])
        dated = [t for t in tables if re.search(r"20\d{6}", t)]
        if not dated:
            results.append({"status": "blocked", "library": lib, "reason": "No date-stamped TAQ tables discovered"})
            continue
        sample_tables = dated[:: max(1, len(dated) // 24)][:24]
        for tbl in sample_tables:
            cols = table_columns(schema, lib, tbl)
            wanted = [c for c in ["sym_root", "symbol", "date", "time_m", "price", "size", "bid", "ask", "ex", "cond"] if c in cols]
            if not wanted:
                wanted = cols[:8]
            sql = f"select {', '.join(map(safe_ident, wanted))} from {safe_table(lib, tbl)} limit 250000"
            out = paths.raw / "wrds" / "taq" / f"{lib}_{tbl}_sample.parquet"
            results.append(fetch_sql_to_parquet(sql, out, retries=0))
        break
    return results


def download_ff_factors(paths: Paths) -> list[dict]:
    specs = {
        "ff3_monthly": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip",
        "ff5_monthly": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip",
        "momentum_monthly": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip",
    }
    results = []
    for name, url in specs.items():
        out = paths.raw / "public" / f"{name}.csv"
        if out.exists() and out.stat().st_size > 0:
            results.append({"status": "skipped_existing", "name": name, "path": str(out)})
            continue
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
                out.write_bytes(zf.read(csv_name))
            results.append({"status": "ok", "name": name, "path": str(out), "bytes": out.stat().st_size})
        except Exception as exc:
            results.append({"status": "failed", "name": name, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
    return results


def run_extract(paths: Paths, cfg: dict, schema: dict, include_taq: bool) -> None:
    all_results: dict[str, list[dict]] = {}
    all_results["insiders"] = extract_insiders(paths, cfg, schema)
    all_results["insider_support"] = extract_insider_support_tables(paths, cfg, schema)
    all_results["crsp_monthly"] = extract_crsp_monthly(paths, cfg, schema)
    all_results["crsp_daily"] = extract_crsp_daily(paths, cfg, schema)
    all_results["crsp_names_ccm"] = extract_crsp_names_and_ccm(paths, cfg, schema)
    all_results["compustat"] = extract_compustat(paths, cfg, schema)
    all_results["fama_french"] = download_ff_factors(paths)
    if include_taq:
        all_results["taq"] = extract_taq_calibration(paths, cfg, schema)
    manifest(paths, "extract_summary", {"status": "ok", "results": all_results})


def read_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = []
    for file in files:
        try:
            frames.append(pd.read_parquet(file))
        except Exception:
            logging.exception("failed reading %s", file)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def normalize_cusip6(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
        .str.slice(0, 6)
        .replace({"": np.nan, "NAN": np.nan, "NONE": np.nan})
    )


ALIASES = {
    "dcn": ["dcn", "accession_number", "document_control_number"],
    "seqnum": ["seqnum", "sequence", "seq"],
    "personid": ["personid", "reporting_owner_cik", "owner_cik"],
    "secid": ["secid", "issuer_secid"],
    "formtype": ["formtype", "form_type"],
    "filing_date": ["filing_date", "fdate", "filedate", "rdate"],
    "transaction_date": ["transaction_date", "trandate", "tdate"],
    "transaction_code": ["transaction_code", "trancode", "trans_code", "code"],
    "ad_flag": ["acquired_disposed_flag", "adcode", "acq_disp", "acqdisp"],
    "shares": ["shares_acquired_or_disposed", "shares", "shrs", "transaction_shares"],
    "price": ["transaction_price", "price", "tprice", "pricepershare"],
    "cusip": ["issuer_cusip", "cusip", "cusip6", "cusip8"],
    "ticker": ["issuer_ticker", "ticker", "tic"],
    "cik": ["issuer_cik", "cik"],
    "owner": ["reporting_owner_name", "rptownname", "ownername", "insider_name"],
    "role_title": ["officer_title", "title", "relationship"],
    "rolecode1": ["rolecode1"],
    "rolecode2": ["rolecode2"],
    "rolecode3": ["rolecode3"],
    "rolecode4": ["rolecode4"],
    "ownership_form": ["ownership", "direct_or_indirect_ownership"],
    "shares_held": ["sharesheld", "shares_owned_following_transaction"],
    "security_title": ["sectitle", "security_title"],
    "cleanse": ["cleanse"],
    "amendment_flag": ["amend", "amendment_flag"],
    "is_director": ["is_director", "director"],
    "is_officer": ["is_officer", "officer"],
    "is_ten_percent_owner": ["is_ten_percent_owner", "ten_percent_owner", "tenpercentowner"],
    "tenb5": ["rule_10b5_1_indicator", "tenb5", "rule10b5", "planned_trade"],
}


def find_col(df: pd.DataFrame, canonical: str) -> str | None:
    lower_to_col = {str(c).lower(): c for c in df.columns}
    for alias in ALIASES[canonical]:
        if alias.lower() in lower_to_col:
            return lower_to_col[alias.lower()]
    return None


def normalize_insiders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = pd.DataFrame(index=df.index)
    for key in ALIASES:
        col = find_col(df, key)
        if col is not None:
            out[key] = df[col]
        else:
            out[key] = np.nan
    out["filing_date"] = pd.to_datetime(out["filing_date"], errors="coerce")
    out["transaction_date"] = pd.to_datetime(out["transaction_date"], errors="coerce")
    for col in ["shares", "price"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    code = out["transaction_code"].astype(str).str.upper().str.strip()
    ad = out["ad_flag"].astype(str).str.upper().str.strip()
    sign = np.select(
        [ad.eq("A") | code.isin(["P", "A"]), ad.eq("D") | code.isin(["S", "D"])],
        [1.0, -1.0],
        default=np.nan,
    )
    out["trade_sign"] = sign
    out["dollar_value"] = out["trade_sign"] * out["shares"].abs() * out["price"].abs()
    out["abs_dollar_value"] = out["dollar_value"].abs()
    out["filing_lag_days"] = (out["filing_date"] - out["transaction_date"]).dt.days
    out["code"] = code
    out["is_open_market"] = code.isin(["P", "S"])
    out["is_grant_or_award_code"] = code.isin(["A"])
    out["is_option_or_derivative_code"] = code.isin(["M", "X", "C"])
    out["is_tax_or_disposition_code"] = code.isin(["F", "D"])
    out["is_gift_or_other_code"] = code.isin(["G", "J", "W", "Z"])
    out["is_mechanical_code"] = (
        out["is_grant_or_award_code"]
        | out["is_option_or_derivative_code"]
        | out["is_tax_or_disposition_code"]
        | out["is_gift_or_other_code"]
    )
    out["is_10b5"] = out["tenb5"].astype(str).str.upper().isin(["1", "Y", "YES", "TRUE", "T"])
    role_blob = (
        out[["role_title", "rolecode1", "rolecode2", "rolecode3", "rolecode4"]]
        .astype(str)
        .agg(" ".join, axis=1)
        .str.upper()
    )
    out["is_ceo_cfo"] = role_blob.str.contains(r"\b(?:CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL)\b", regex=True)
    out["is_director_role"] = role_blob.str.contains(r"\b(?:D|DIR|DIRECTOR)\b", regex=True)
    out["is_officer_role"] = role_blob.str.contains(r"\b(?:O|OFFICER|CEO|CFO|PRES|VP|CHIEF)\b", regex=True)
    out["missing_price"] = out["price"].isna()
    out["missing_shares"] = out["shares"].isna()
    out["value_to_holdings"] = out["shares"].abs() / pd.to_numeric(out["shares_held"], errors="coerce").abs()
    out["ym"] = out["filing_date"].dt.to_period("M").astype(str)
    return out


def enrich_with_support_tables(paths: Paths, insiders: pd.DataFrame) -> pd.DataFrame:
    if insiders.empty:
        return insiders
    out = insiders.copy()
    for col in ["dcn", "seqnum", "personid", "secid"]:
        if col in out:
            out[col] = out[col].astype(str)

    rule = read_parquet_dir(paths.raw / "wrds" / "insider_support")
    if not rule.empty and {"dcn", "seqnum", "flag"}.issubset(rule.columns):
        r = rule[[c for c in ["dcn", "seqnum", "table_type", "flag"] if c in rule.columns]].copy()
        r["dcn"] = r["dcn"].astype(str)
        r["seqnum"] = r["seqnum"].astype(str)
        r["rule10b5_flag"] = r["flag"].astype(str).str.upper().isin(["1", "Y", "YES", "TRUE", "T"])
        r = r.groupby(["dcn", "seqnum"], as_index=False)["rule10b5_flag"].max()
        out = out.merge(r, on=["dcn", "seqnum"], how="left")
        out["is_10b5"] = out["is_10b5"].fillna(False) | out["rule10b5_flag"].fillna(False)
    else:
        out["rule10b5_flag"] = False

    table2 = read_parquet_dir(paths.raw / "wrds" / "insiders_table2")
    if not table2.empty and "dcn" in table2.columns:
        t2 = table2.copy()
        for col in ["dcn", "personid", "secid"]:
            if col in t2:
                t2[col] = t2[col].astype(str)
        t2["derivative_shares"] = pd.to_numeric(t2.get("shares", t2.get("num_deriv")), errors="coerce")
        t2["derivative_price"] = pd.to_numeric(t2.get("sprice", t2.get("xprice")), errors="coerce")
        group_cols = [c for c in ["dcn", "personid", "secid"] if c in t2.columns]
        deriv = (
            t2.groupby(group_cols, dropna=False)
            .agg(
                derivative_record_count=("dcn", "size"),
                derivative_abs_value=("derivative_shares", lambda x: float(np.nansum(np.abs(x)))),
                derivative_mean_price=("derivative_price", "mean"),
            )
            .reset_index()
        )
        out = out.merge(deriv, on=group_cols, how="left")
    for col in ["derivative_record_count", "derivative_abs_value", "derivative_mean_price"]:
        if col not in out:
            out[col] = 0.0
    out["has_derivative_companion"] = out["derivative_record_count"].fillna(0).gt(0)

    amend_files = sorted((paths.raw / "wrds" / "insider_support").glob("amend_*.parquet"))
    amend_frames = []
    for file in amend_files:
        with contextlib.suppress(Exception):
            amend_frames.append(pd.read_parquet(file))
    if amend_frames:
        amend = pd.concat(amend_frames, ignore_index=True, sort=False)
        dcn_set = set(amend.get("dcn", pd.Series(dtype=str)).astype(str))
        amend_dcn_set = set(amend.get("amend_dcn", pd.Series(dtype=str)).astype(str))
        out["is_amended_record"] = out["dcn"].astype(str).isin(dcn_set | amend_dcn_set)
    else:
        out["is_amended_record"] = out["amendment_flag"].astype(str).str.upper().isin(["1", "Y", "TRUE", "A"])

    out["is_routine_or_mechanical"] = (
        out["is_mechanical_code"].fillna(False)
        | out["is_10b5"].fillna(False)
        | out["has_derivative_companion"].fillna(False)
        | out["is_amended_record"].fillna(False)
    )
    return out


def build_features(paths: Paths) -> None:
    insiders_raw = read_parquet_dir(paths.raw / "wrds" / "insiders")
    if insiders_raw.empty:
        manifest(paths, "feature_build", {"status": "blocked", "reason": "no insider parquet shards"})
        return
    insiders = normalize_insiders(insiders_raw)
    insiders = enrich_with_support_tables(paths, insiders)
    write_parquet_atomic(insiders, paths.processed / "features" / "insider_transactions_normalized.parquet")

    # CRSP monthly market cap support.
    crsp = read_parquet_dir(paths.raw / "wrds" / "crsp_msf")
    if not crsp.empty:
        crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce")
        for col in ["prc", "shrout", "ret"]:
            if col in crsp.columns:
                crsp[col] = pd.to_numeric(crsp[col], errors="coerce")
        if {"prc", "shrout"}.issubset(crsp.columns):
            crsp["mktcap"] = crsp["prc"].abs() * crsp["shrout"] * 1000.0
        crsp["ym"] = crsp["date"].dt.to_period("M").astype(str)
        write_parquet_atomic(crsp, paths.processed / "features" / "crsp_monthly_features.parquet")

    firm_month = (
        insiders.groupby("ym", dropna=True)
        .agg(
            liis_raw=("dollar_value", "sum"),
            liis_abs=("abs_dollar_value", "sum"),
            insider_trade_count=("dollar_value", "size"),
            buy_count=("trade_sign", lambda x: int((x > 0).sum())),
            sell_count=("trade_sign", lambda x: int((x < 0).sum())),
            routine_share=("is_mechanical_code", "mean"),
            tenb5_share=("is_10b5", "mean"),
        )
        .reset_index()
    )
    write_parquet_atomic(firm_month, paths.processed / "panels" / "aggregate_insider_month.parquet")
    manifest(
        paths,
        "feature_build",
        {
            "status": "ok",
            "normalized_rows": int(len(insiders)),
            "aggregate_month_rows": int(len(firm_month)),
            "columns": list(insiders.columns),
        },
    )


def train_torch_gpu_classifier(
    df: pd.DataFrame,
    features: list[str],
    train_mask: np.ndarray,
    paths: Paths,
    max_train_rows: int = 3_000_000,
) -> tuple[np.ndarray | None, dict]:
    """Train a compact PyTorch classifier on the A100 when available.

    This is the GPU branch used when LightGBM's installed build lacks GPU
    support. It is intentionally small: the task is weak-supervised tabular
    denoising, so a compact MLP gives us GPU execution without turning the
    pipeline into an unbounded deep-learning run.
    """

    payload: dict[str, object] = {"attempted": True}
    try:
        import torch
        import torch.nn as nn
    except Exception as exc:
        payload.update({"status": "skipped", "reason": f"torch import failed: {type(exc).__name__}"})
        return None, payload

    if not torch.cuda.is_available():
        payload.update({"status": "skipped", "reason": "cuda unavailable"})
        return None, payload

    y_all = df["weak_informative"].to_numpy(dtype=np.float32, copy=False)
    train_idx = np.flatnonzero(train_mask & np.isfinite(y_all))
    if len(train_idx) < 100 or len(np.unique(y_all[train_idx])) < 2:
        payload.update({"status": "skipped", "reason": "insufficient labeled variation"})
        return None, payload

    rng = np.random.default_rng(137)
    if len(train_idx) > max_train_rows:
        train_idx = rng.choice(train_idx, size=max_train_rows, replace=False)
    train_idx = np.sort(train_idx)

    x_train = df.iloc[train_idx][features].to_numpy(dtype=np.float32, copy=True)
    y_train = y_all[train_idx].astype(np.float32, copy=True)

    device = torch.device("cuda")
    torch.manual_seed(137)
    model = nn.Sequential(
        nn.Linear(len(features), 64),
        nn.ReLU(),
        nn.BatchNorm1d(64),
        nn.Linear(64, 32),
        nn.ReLU(),
        nn.Linear(32, 1),
    ).to(device)
    pos = float(y_train.sum())
    neg = float(len(y_train) - pos)
    pos_weight = torch.tensor([max(1.0, neg / max(pos, 1.0))], device=device)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    opt = torch.optim.AdamW(model.parameters(), lr=2e-3, weight_decay=1e-4)

    batch_size = 262_144
    losses: list[float] = []
    for epoch in range(3):
        order = rng.permutation(len(y_train))
        epoch_loss = 0.0
        seen = 0
        model.train()
        for start in range(0, len(order), batch_size):
            idx = order[start : start + batch_size]
            xb = torch.as_tensor(x_train[idx], device=device)
            yb = torch.as_tensor(y_train[idx, None], device=device)
            opt.zero_grad(set_to_none=True)
            loss = loss_fn(model(xb), yb)
            loss.backward()
            opt.step()
            epoch_loss += float(loss.detach().cpu()) * len(idx)
            seen += len(idx)
        losses.append(epoch_loss / max(seen, 1))

    x_all = df[features].to_numpy(dtype=np.float32, copy=False)
    probs = np.empty(len(df), dtype=np.float32)
    model.eval()
    with torch.no_grad():
        for start in range(0, len(df), 1_000_000):
            xb = torch.as_tensor(x_all[start : start + 1_000_000], device=device)
            probs[start : start + len(xb)] = torch.sigmoid(model(xb)).squeeze(1).detach().cpu().numpy()

    model_path = paths.models / "stage1_torch_gpu_mlp.pt"
    torch.save(
        {
            "state_dict": model.state_dict(),
            "features": features,
            "losses": losses,
            "train_rows": int(len(y_train)),
        },
        model_path,
    )
    payload.update(
        {
            "status": "ok",
            "device": torch.cuda.get_device_name(0),
            "train_rows": int(len(y_train)),
            "epochs": 3,
            "losses": losses,
            "model_path": str(model_path),
        }
    )
    return probs, payload


def train_models(paths: Paths, cfg: dict) -> None:
    tx_path = paths.processed / "features" / "insider_transactions_normalized.parquet"
    if not tx_path.exists():
        manifest(paths, "model_training", {"status": "blocked", "reason": "missing normalized transaction features"})
        return
    df = pd.read_parquet(tx_path)
    if df.empty or "filing_date" not in df:
        manifest(paths, "model_training", {"status": "blocked", "reason": "empty transaction feature panel"})
        return
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df.dropna(subset=["filing_date"]).copy()
    df["weak_informative"] = (
        df["is_open_market"].fillna(False)
        & ~df["is_10b5"].fillna(False)
        & ~df["is_routine_or_mechanical"].fillna(df["is_mechanical_code"]).fillna(False)
        & df["abs_dollar_value"].fillna(0).gt(df["abs_dollar_value"].quantile(0.60))
    ).astype(int)
    df["log_abs_dollar"] = np.log1p(df["abs_dollar_value"].fillna(0))
    df["filing_lag_days_clip"] = df["filing_lag_days"].clip(-30, 365).fillna(999)
    df["log_value_to_holdings"] = np.log1p(pd.to_numeric(df.get("value_to_holdings"), errors="coerce").clip(lower=0, upper=1000).fillna(0))
    df["log_derivative_companion"] = np.log1p(pd.to_numeric(df.get("derivative_record_count"), errors="coerce").fillna(0))
    df["post_2023_rule10b5_era"] = (df["filing_date"] >= pd.Timestamp("2023-04-01")).astype(int)
    bool_features = [
        "is_open_market",
        "is_mechanical_code",
        "is_10b5",
        "is_ceo_cfo",
        "has_derivative_companion",
        "is_amended_record",
        "missing_price",
        "missing_shares",
        "is_tax_or_disposition_code",
        "is_option_or_derivative_code",
    ]
    for col in bool_features:
        if col not in df:
            df[col] = False
        df[col] = df[col].fillna(False).astype(int)
    features = [
        "log_abs_dollar",
        "filing_lag_days_clip",
        "log_value_to_holdings",
        "log_derivative_companion",
        "post_2023_rule10b5_era",
        *bool_features,
    ]
    for col in features:
        df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype("float32")
    df["weak_informative"] = pd.to_numeric(df["weak_informative"], errors="coerce").fillna(0).astype("int8")
    train_end = pd.Timestamp(cfg["models"]["temporal_split"]["train_end"])
    train_mask = (df["filing_date"] <= train_end).to_numpy()
    train = df[train_mask].copy()
    score = df.copy()

    model_payload: dict[str, object] = {"rows": int(len(df)), "train_rows": int(len(train)), "features": features}
    elastic_status = "not_run"
    if train["weak_informative"].nunique() >= 2 and len(train) >= 100:
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.pipeline import make_pipeline
            from sklearn.preprocessing import StandardScaler

            elastic_train = train
            if len(elastic_train) > 500_000:
                elastic_train = elastic_train.sample(n=500_000, random_state=137)
            elastic = make_pipeline(
                StandardScaler(),
                LogisticRegression(
                    penalty="elasticnet",
                    solver="saga",
                    l1_ratio=0.5,
                    C=0.25,
                    max_iter=100,
                    n_jobs=1,
                    random_state=137,
                ),
            )
            elastic.fit(elastic_train[features], elastic_train["weak_informative"])
            df["p_informative_elastic_net"] = elastic.predict_proba(score[features])[:, 1]
            elastic_status = "ok"
        except Exception as exc:
            logging.exception("Elastic Net logistic training failed")
            elastic_status = f"failed:{type(exc).__name__}"
    model_payload["elastic_net_status"] = elastic_status

    if train["weak_informative"].nunique() < 2 or len(train) < 100:
        df["p_informative"] = df["weak_informative"].astype(float)
        model_payload["status"] = "fallback_rules_only"
    else:
        try:
            import lightgbm as lgb

            lightgbm_backend = "gpu"
            clf = lgb.LGBMClassifier(
                n_estimators=400,
                learning_rate=0.03,
                num_leaves=31,
                subsample=0.85,
                colsample_bytree=0.85,
                random_state=137,
                n_jobs=max(1, min(20, os.cpu_count() or 1)),
                device_type="gpu",
            )
            try:
                clf.fit(train[features], train["weak_informative"])
            except Exception:
                logging.exception("LightGBM GPU failed; retrying CPU")
                lightgbm_backend = "cpu"
                clf.set_params(device_type="cpu")
                clf.fit(train[features], train["weak_informative"])
            df["p_informative"] = clf.predict_proba(score[features])[:, 1]
            model_payload["status"] = f"ok_lightgbm_{lightgbm_backend}"
            model_payload["lightgbm_backend"] = lightgbm_backend
            model_payload["feature_importance"] = dict(zip(features, map(float, clf.feature_importances_)))
            try:
                import shap

                shap_sample = train[features].sample(n=min(100_000, len(train)), random_state=137)
                explainer = shap.TreeExplainer(clf)
                shap_values = explainer.shap_values(shap_sample)
                if isinstance(shap_values, list):
                    shap_values = shap_values[-1]
                mean_abs = np.abs(np.asarray(shap_values)).mean(axis=0)
                pd.DataFrame({"feature": features, "mean_abs_shap": mean_abs}).sort_values(
                    "mean_abs_shap", ascending=False
                ).to_csv(paths.tables / "stage1_shap_summary.csv", index=False)
                model_payload["shap_summary_status"] = "ok"
            except Exception as exc:
                logging.exception("SHAP summary failed")
                model_payload["shap_summary_status"] = f"failed:{type(exc).__name__}"
            with (paths.models / "stage1_lightgbm_model.txt").open("w", encoding="utf-8") as fh:
                fh.write(str(clf))
        except Exception as exc:
            logging.exception("LightGBM training failed; using rules-only fallback")
            df["p_informative"] = df["weak_informative"].astype(float)
            model_payload["status"] = "fallback_after_error"
            model_payload["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"

    gpu_probs, gpu_payload = train_torch_gpu_classifier(df, features, train_mask, paths)
    model_payload["torch_gpu_mlp"] = gpu_payload
    if gpu_probs is not None:
        df["p_informative_gpu_mlp"] = gpu_probs
        if str(model_payload.get("status", "")).startswith("ok_lightgbm"):
            df["p_informative"] = 0.70 * df["p_informative"].astype("float32") + 0.30 * df["p_informative_gpu_mlp"]
            model_payload["final_score_source"] = f"lightgbm_{model_payload.get('lightgbm_backend', 'unknown')}_torch_gpu_mlp_blend"
        else:
            df["p_informative"] = df["p_informative_gpu_mlp"]
            model_payload["final_score_source"] = "torch_gpu_mlp"

    pd.DataFrame(
        [
            {"model": "rules_only", "role": "transparent baseline", "status": "available"},
            {"model": "elastic_net_logistic", "role": "sparse benchmark", "status": elastic_status},
            {"model": "lightgbm", "role": "main tabular workhorse", "status": model_payload.get("status")},
            {"model": "torch_gpu_mlp", "role": "GPU sequence/tabular extension", "status": gpu_payload.get("status")},
            {"model": "blend", "role": "production score", "status": model_payload.get("final_score_source", model_payload.get("status"))},
        ]
    ).to_csv(paths.tables / "model_variant_table.csv", index=False)
    df["signed_info_value"] = df["trade_sign"].fillna(0) * df["p_informative"].fillna(0) * df["abs_dollar_value"].fillna(0)
    write_parquet_atomic(df, paths.processed / "scores" / "transaction_scores.parquet")
    manifest(paths, "model_training", model_payload)


def run_backtests(paths: Paths) -> None:
    score_path = paths.processed / "scores" / "transaction_scores.parquet"
    if not score_path.exists():
        manifest(paths, "backtest", {"status": "blocked", "reason": "missing transaction scores"})
        return
    df = pd.read_parquet(score_path)
    if df.empty:
        manifest(paths, "backtest", {"status": "blocked", "reason": "empty transaction scores"})
        return
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["ym"] = df["filing_date"].dt.to_period("M").astype(str)
    df["cusip6"] = normalize_cusip6(df["cusip"]) if "cusip" in df.columns else np.nan
    df = df.dropna(subset=["ym", "cusip6"]).copy()
    df["tx_id"] = np.arange(len(df), dtype=np.int64)

    names_path = paths.raw / "wrds" / "crsp_names" / "msenames.parquet"
    crsp_path = paths.processed / "features" / "crsp_monthly_features.parquet"
    if not names_path.exists() or not crsp_path.exists():
        # Fallback keeps the pipeline alive if mapping support is absent.
        panel = (
            df.groupby(["ym", "cusip6"], dropna=True)
            .agg(
                liis_net=("signed_info_value", "sum"),
                liis_buy=("signed_info_value", lambda x: float(x[x > 0].sum())),
                liis_sell=("signed_info_value", lambda x: float(x[x < 0].sum())),
                trade_count=("signed_info_value", "size"),
                mean_p_info=("p_informative", "mean"),
            )
            .reset_index()
        )
        write_parquet_atomic(panel, paths.processed / "panels" / "liis_panel.parquet")
        panel.describe(include="all").transpose().to_csv(paths.tables / "liis_panel_summary.csv")
        manifest(paths, "backtest", {"status": "fallback_no_permno_mapping", "panel_rows": int(len(panel))})
        return

    names = pd.read_parquet(names_path)
    names["namedt"] = pd.to_datetime(names.get("namedt"), errors="coerce")
    names["nameendt"] = pd.to_datetime(names.get("nameendt"), errors="coerce").fillna(pd.Timestamp("2100-12-31"))
    names["permno"] = pd.to_numeric(names.get("permno"), errors="coerce")
    names["shrcd"] = pd.to_numeric(names.get("shrcd"), errors="coerce") if "shrcd" in names else np.nan
    names["cusip6"] = normalize_cusip6(names["ncusip"] if "ncusip" in names else names.get("cusip", pd.Series(index=names.index)))
    names = names.dropna(subset=["permno", "cusip6", "namedt"]).copy()
    names["start_ym"] = names["namedt"].dt.to_period("M")
    names["end_ym"] = names["nameendt"].clip(upper=pd.Timestamp("2026-12-31")).dt.to_period("M")
    names = names.sort_values(["cusip6", "start_ym", "end_ym", "permno"])

    # Build a date-valid CUSIP6-month-PERMNO bridge. This is much smaller than
    # transaction-level range joins and keeps the backtest restartable.
    bridge_rows = []
    for row in names[["cusip6", "permno", "shrcd", "start_ym", "end_ym"]].itertuples(index=False):
        months = pd.period_range(row.start_ym, row.end_ym, freq="M")
        if len(months) > 360:
            months = months[-360:]
        bridge_rows.append(
            pd.DataFrame(
                {
                    "cusip6": row.cusip6,
                    "permno": int(row.permno),
                    "shrcd": row.shrcd,
                    "ym": months.astype(str),
                }
            )
        )
    bridge = pd.concat(bridge_rows, ignore_index=True) if bridge_rows else pd.DataFrame(columns=["cusip6", "permno", "shrcd", "ym"])
    bridge["common_stock_priority"] = bridge["shrcd"].isin([10, 11]).astype(int)
    bridge = bridge.sort_values(["cusip6", "ym", "common_stock_priority"], ascending=[True, True, False])
    bridge = bridge.drop_duplicates(["cusip6", "ym"], keep="first")

    mapped = df.merge(bridge[["cusip6", "ym", "permno"]], on=["cusip6", "ym"], how="left")
    mapped = mapped.dropna(subset=["permno"]).copy()
    mapped["permno"] = mapped["permno"].astype("int64")
    for col in ["is_ceo_cfo", "is_10b5", "is_open_market", "has_derivative_companion", "is_amended_record"]:
        if col not in mapped:
            mapped[col] = False
        mapped[col] = mapped[col].fillna(False).astype(bool)
    mapped["signed_info_ceo_cfo"] = mapped["signed_info_value"].where(mapped["is_ceo_cfo"], 0.0)
    mapped["signed_info_nonplan"] = mapped["signed_info_value"].where(~mapped["is_10b5"], 0.0)
    mapped["signed_info_open_market"] = mapped["signed_info_value"].where(mapped["is_open_market"], 0.0)
    mapped["signed_info_derivative_linked"] = mapped["signed_info_value"].where(mapped["has_derivative_companion"], 0.0)
    mapped["signed_info_amendment_strict"] = mapped["signed_info_value"].where(~mapped["is_amended_record"], 0.0)
    mapped["buy_owner"] = mapped["owner"].where(mapped["trade_sign"].fillna(0).gt(0))
    mapped["sell_owner"] = mapped["owner"].where(mapped["trade_sign"].fillna(0).lt(0))

    panel = (
        mapped.groupby(["permno", "ym"], dropna=True)
        .agg(
            liis_net=("signed_info_value", "sum"),
            liis_buy=("signed_info_value", lambda x: float(x[x > 0].sum())),
            liis_sell=("signed_info_value", lambda x: float(x[x < 0].sum())),
            liis_ceo_cfo=("signed_info_ceo_cfo", "sum"),
            liis_nonplan=("signed_info_nonplan", "sum"),
            liis_open_market=("signed_info_open_market", "sum"),
            liis_derivative_linked=("signed_info_derivative_linked", "sum"),
            liis_amendment_strict=("signed_info_amendment_strict", "sum"),
            trade_count=("signed_info_value", "size"),
            buy_owner_count=("buy_owner", pd.Series.nunique),
            sell_owner_count=("sell_owner", pd.Series.nunique),
            mean_p_info=("p_informative", "mean"),
        )
        .reset_index()
    )
    panel["liis_cluster"] = panel[["buy_owner_count", "sell_owner_count"]].max(axis=1)
    total_sides = panel["buy_owner_count"] + panel["sell_owner_count"]
    buy_share = panel["buy_owner_count"] / total_sides.replace(0, np.nan)
    panel["liis_disagreement"] = -(
        buy_share * np.log(buy_share) + (1 - buy_share) * np.log(1 - buy_share)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)

    crsp = pd.read_parquet(crsp_path)
    crsp["permno"] = pd.to_numeric(crsp["permno"], errors="coerce")
    crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce")
    crsp["ym"] = crsp["date"].dt.to_period("M").astype(str)
    crsp["ret"] = pd.to_numeric(crsp.get("ret"), errors="coerce")
    crsp["mktcap"] = pd.to_numeric(crsp.get("mktcap"), errors="coerce")
    crsp = crsp.dropna(subset=["permno", "ym"]).sort_values(["permno", "date"])
    crsp["ret_fwd_1m"] = crsp.groupby("permno")["ret"].shift(-1)
    crsp["mktcap_lag"] = crsp.groupby("permno")["mktcap"].shift(1)
    crsp["permno"] = crsp["permno"].astype("int64")
    crsp_month = crsp[["permno", "ym", "ret", "ret_fwd_1m", "mktcap", "mktcap_lag"]].drop_duplicates(["permno", "ym"], keep="last")

    panel = panel.merge(crsp_month, on=["permno", "ym"], how="left")
    denom = panel["mktcap_lag"].where(panel["mktcap_lag"].gt(0), panel["mktcap"].where(panel["mktcap"].gt(0)))
    panel["liis_net_scaled"] = panel["liis_net"] / denom
    panel["liis_buy_scaled"] = panel["liis_buy"] / denom
    panel["liis_sell_scaled"] = panel["liis_sell"] / denom
    for col in ["liis_ceo_cfo", "liis_nonplan", "liis_open_market", "liis_derivative_linked", "liis_amendment_strict"]:
        panel[f"{col}_scaled"] = panel[col] / denom
    panel = panel.replace([np.inf, -np.inf], np.nan)

    def decile_month(group: pd.DataFrame) -> pd.Series:
        valid = group["liis_net_scaled"].replace([np.inf, -np.inf], np.nan)
        if valid.notna().sum() < 20 or valid.nunique(dropna=True) < 10:
            return pd.Series(np.nan, index=group.index)
        return pd.qcut(valid.rank(method="first"), 10, labels=False, duplicates="drop").astype(float) + 1

    panel["liis_decile"] = panel.groupby("ym", group_keys=False).apply(decile_month)
    write_parquet_atomic(panel, paths.processed / "panels" / "liis_panel.parquet")

    decile_returns = (
        panel.dropna(subset=["liis_decile", "ret_fwd_1m"])
        .groupby(["ym", "liis_decile"])["ret_fwd_1m"]
        .mean()
        .reset_index()
    )
    decile_wide = decile_returns.pivot(index="ym", columns="liis_decile", values="ret_fwd_1m").sort_index()
    if {1.0, 10.0}.issubset(set(decile_wide.columns)):
        decile_wide["long_short_10_1"] = decile_wide[10.0] - decile_wide[1.0]
        decile_wide["cum_long_short_10_1"] = (1 + decile_wide["long_short_10_1"].fillna(0)).cumprod() - 1
    decile_wide.to_csv(paths.tables / "decile_returns.csv")

    risk_rows = []
    if "long_short_10_1" in decile_wide:
        ls = decile_wide["long_short_10_1"].dropna()
        wealth = (1 + ls).cumprod()
        drawdown = wealth / wealth.cummax() - 1
        risk_rows.append(
            {
                "series": "long_short_10_1",
                "months": int(len(ls)),
                "mean_monthly": float(ls.mean()),
                "vol_monthly": float(ls.std(ddof=1)),
                "ann_return_approx": float(ls.mean() * 12),
                "ann_vol_approx": float(ls.std(ddof=1) * math.sqrt(12)),
                "sharpe_approx": float((ls.mean() * 12) / (ls.std(ddof=1) * math.sqrt(12))) if ls.std(ddof=1) else np.nan,
                "max_drawdown": float(drawdown.min()),
            }
        )
        pd.DataFrame({"ym": ls.index, "long_short_10_1": ls.values, "wealth": wealth.values, "drawdown": drawdown.values}).to_csv(
            paths.tables / "drawdown_turnover.csv", index=False
        )
    pd.DataFrame(risk_rows).to_csv(paths.tables / "performance_summary.csv", index=False)

    ff_path = paths.raw / "public" / "ff3_monthly.csv"
    if ff_path.exists() and "long_short_10_1" in decile_wide:
        try:
            raw_lines = ff_path.read_text(encoding="latin1").splitlines()
            rows_ff = []
            header = None
            for line in raw_lines:
                parts = [p.strip() for p in line.split(",")]
                if not parts or not parts[0]:
                    if "Mkt-RF" in parts:
                        header = ["Date", *parts[1:]]
                    continue
                if parts[0].startswith("Date") or "Mkt-RF" in parts:
                    if not parts[0]:
                        parts[0] = "Date"
                    header = parts
                    continue
                if re.fullmatch(r"\d{6}", parts[0]) and header:
                    rows_ff.append(parts[: len(header)])
            ff = pd.DataFrame(rows_ff, columns=header)
            if not ff.empty:
                ff["ym"] = pd.to_datetime(ff["Date"], format="%Y%m").dt.to_period("M").astype(str)
                for c in ["Mkt-RF", "SMB", "HML", "RF"]:
                    ff[c] = pd.to_numeric(ff[c], errors="coerce") / 100.0
                ret = decile_wide.reset_index().rename(columns={"index": "ym"})
                ret["ym"] = ret["ym"].astype(str)
                merged_ff = ret[["ym", "long_short_10_1"]].merge(ff[["ym", "Mkt-RF", "SMB", "HML", "RF"]], on="ym", how="inner").dropna()
                if len(merged_ff) > 24:
                    y = merged_ff["long_short_10_1"].to_numpy(float)
                    x = np.column_stack(
                        [
                            np.ones(len(merged_ff)),
                            merged_ff["Mkt-RF"].to_numpy(float),
                            merged_ff["SMB"].to_numpy(float),
                            merged_ff["HML"].to_numpy(float),
                        ]
                    )
                    beta = np.linalg.lstsq(x, y, rcond=None)[0]
                    pd.DataFrame(
                        [
                            {"term": "alpha", "loading": beta[0], "months": len(merged_ff)},
                            {"term": "Mkt-RF", "loading": beta[1], "months": len(merged_ff)},
                            {"term": "SMB", "loading": beta[2], "months": len(merged_ff)},
                            {"term": "HML", "loading": beta[3], "months": len(merged_ff)},
                        ]
                    ).to_csv(paths.tables / "factor_loadings.csv", index=False)
        except Exception:
            logging.exception("failed factor loading calculation")

    robustness_rows = []

    def signal_long_short(signal: str, label: str, data: pd.DataFrame | None = None) -> None:
        d = (panel if data is None else data).dropna(subset=[signal, "ret_fwd_1m", "ym"]).copy()
        if d.empty:
            robustness_rows.append({"test": label, "status": "empty"})
            return
        def local_decile(g: pd.DataFrame) -> pd.Series:
            x = g[signal].replace([np.inf, -np.inf], np.nan)
            if x.notna().sum() < 20 or x.nunique(dropna=True) < 10:
                return pd.Series(np.nan, index=g.index)
            return pd.qcut(x.rank(method="first"), 10, labels=False, duplicates="drop").astype(float) + 1
        d["decile"] = d.groupby("ym", group_keys=False).apply(local_decile)
        spread = (
            d.dropna(subset=["decile"])
            .groupby(["ym", "decile"])["ret_fwd_1m"]
            .mean()
            .unstack()
        )
        if 1.0 in spread.columns and 10.0 in spread.columns:
            ls = (spread[10.0] - spread[1.0]).dropna()
            if len(ls) > 1:
                robustness_rows.append(
                    {
                        "test": label,
                        "status": "ok",
                        "months": int(len(ls)),
                        "mean_long_short": float(ls.mean()),
                        "t_stat": float(ls.mean() / (ls.std(ddof=1) / math.sqrt(len(ls)))),
                    }
                )
                return
        robustness_rows.append({"test": label, "status": "insufficient_deciles"})

    signal_long_short("liis_net_scaled", "main_liis_net")
    signal_long_short("liis_buy_scaled", "buys_only")
    signal_long_short("liis_sell_scaled", "sells_only")
    signal_long_short("liis_ceo_cfo_scaled", "ceo_cfo_only")
    signal_long_short("liis_nonplan_scaled", "non_10b5_only")
    signal_long_short("liis_open_market_scaled", "open_market_only")
    signal_long_short("liis_amendment_strict_scaled", "amendment_strict")
    liquid = panel[panel["mktcap_lag"].ge(panel["mktcap_lag"].quantile(0.30))].copy()
    signal_long_short("liis_net_scaled", "liquid_large_70pct", liquid)

    placebo = panel.copy()
    rng = np.random.default_rng(137)
    placebo["liis_net_scaled_placebo"] = (
        placebo.groupby("ym")["liis_net_scaled"]
        .transform(lambda x: pd.Series(rng.permutation(x.to_numpy()), index=x.index))
    )
    signal_long_short("liis_net_scaled_placebo", "placebo_shuffle_within_month", placebo)

    tx_mapped = mapped.dropna(subset=["transaction_date"]).copy()
    tx_mapped["ym"] = pd.to_datetime(tx_mapped["transaction_date"], errors="coerce").dt.to_period("M").astype(str)
    if not tx_mapped.empty:
        tx_panel = (
            tx_mapped.groupby(["permno", "ym"], dropna=True)
            .agg(liis_net_scaled=("signed_info_value", "sum"))
            .reset_index()
            .merge(crsp_month, on=["permno", "ym"], how="left")
        )
        tx_denom = tx_panel["mktcap_lag"].where(tx_panel["mktcap_lag"].gt(0), tx_panel["mktcap"].where(tx_panel["mktcap"].gt(0)))
        tx_panel["liis_net_scaled"] = tx_panel["liis_net_scaled"] / tx_denom
        signal_long_short("liis_net_scaled", "lookahead_transaction_date_alignment", tx_panel)

    robustness = pd.DataFrame(robustness_rows)
    robustness.to_csv(paths.tables / "robustness_summary.csv", index=False)

    # Event-time validation around filing availability using CRSP daily returns.
    daily_dir = paths.raw / "wrds" / "crsp_dsf"
    event_car_status: dict[str, object] = {"status": "not_run"}
    if daily_dir.exists():
        events = (
            mapped.dropna(subset=["filing_date", "permno"])
            .groupby(["permno", "filing_date"], dropna=True)
            .agg(event_liis=("signed_info_value", "sum"), event_p_info=("p_informative", "mean"), event_trade_count=("signed_info_value", "size"))
            .reset_index()
        )
        events["filing_date"] = pd.to_datetime(events["filing_date"], errors="coerce")
        events = events.dropna(subset=["filing_date"])
        if len(events) > 1_000_000:
            events = events.nlargest(1_000_000, "event_liis", keep="all")
        if len(events) > 1000 and events["event_liis"].nunique() >= 10:
            events["event_decile"] = pd.qcut(events["event_liis"].rank(method="first"), 10, labels=False, duplicates="drop").astype(float) + 1
            daily = read_parquet_dir(daily_dir)
            if not daily.empty:
                daily["permno"] = pd.to_numeric(daily["permno"], errors="coerce")
                daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
                daily["ret"] = pd.to_numeric(daily.get("ret"), errors="coerce")
                daily = daily.dropna(subset=["permno", "date", "ret"])[["permno", "date", "ret"]]
                daily["permno"] = daily["permno"].astype("int64")
                car_rows = []
                for tau in range(-5, 6):
                    tmp = events[["permno", "filing_date", "event_decile"]].copy()
                    tmp["date"] = tmp["filing_date"] + pd.offsets.BDay(tau)
                    merged_daily = tmp.merge(daily, on=["permno", "date"], how="left")
                    car_rows.append(
                        merged_daily.groupby(["event_decile"], dropna=True)["ret"]
                        .mean()
                        .rename(tau)
                    )
                event_ret = pd.concat(car_rows, axis=1).sort_index()
                event_car = event_ret.cumsum(axis=1)
                event_ret.to_csv(paths.tables / "event_returns_by_decile.csv")
                event_car.to_csv(paths.tables / "event_car_by_decile.csv")
                event_car_status = {"status": "ok", "events": int(len(events)), "deciles": int(event_car.shape[0])}
        else:
            event_car_status = {"status": "insufficient_events", "events": int(len(events))}

    # Fama-MacBeth-style monthly cross-sectional slopes.
    rows = []
    reg = panel.dropna(subset=["ret_fwd_1m", "liis_net_scaled", "mktcap"])
    reg["log_mktcap"] = np.log(reg["mktcap"].where(reg["mktcap"].gt(0)))
    for ym, g in reg.groupby("ym"):
        g = g.dropna(subset=["ret_fwd_1m", "liis_net_scaled", "log_mktcap"])
        if len(g) < 50:
            continue
        x = np.column_stack([np.ones(len(g)), g["liis_net_scaled"].to_numpy(float), g["log_mktcap"].to_numpy(float)])
        y = g["ret_fwd_1m"].to_numpy(float)
        try:
            beta = np.linalg.lstsq(x, y, rcond=None)[0]
            rows.append({"ym": ym, "alpha": beta[0], "liis_net_scaled": beta[1], "log_mktcap": beta[2], "n": len(g)})
        except np.linalg.LinAlgError:
            continue
    fmb = pd.DataFrame(rows)
    fmb.to_csv(paths.tables / "fama_macbeth_liis.csv", index=False)
    fmb_summary = []
    for col in ["alpha", "liis_net_scaled", "log_mktcap"]:
        if col in fmb and len(fmb[col].dropna()) > 1:
            vals = fmb[col].dropna()
            fmb_summary.append(
                {
                    "term": col,
                    "mean": vals.mean(),
                    "std": vals.std(ddof=1),
                    "t_stat": vals.mean() / (vals.std(ddof=1) / math.sqrt(len(vals))),
                    "months": int(len(vals)),
                }
            )
    pd.DataFrame(fmb_summary).to_csv(paths.tables / "fama_macbeth_summary.csv", index=False)

    panel.describe(include="all").transpose().to_csv(paths.tables / "liis_panel_summary.csv")
    manifest(
        paths,
        "backtest",
        {
            "status": "ok_permno_return_join",
            "panel_rows": int(len(panel)),
            "mapped_transaction_rows": int(len(mapped)),
            "decile_months": int(len(decile_wide)),
            "fmb_months": int(len(fmb)),
            "robustness_tests": int(len(robustness)),
            "event_car": event_car_status,
        },
    )


def make_figures(paths: Paths) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    tx_path = paths.processed / "scores" / "transaction_scores.parquet"
    panel_path = paths.processed / "panels" / "liis_panel.parquet"
    if not tx_path.exists():
        manifest(paths, "figures", {"status": "blocked", "reason": "missing transaction scores"})
        return
    df = pd.read_parquet(tx_path)
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["year"] = df["filing_date"].dt.year

    made: list[str] = []

    def save_all(fig, stem: str) -> None:
        for ext in ["pdf", "svg", "png"]:
            out = paths.figures_static / f"{stem}.{ext}"
            fig.savefig(out, bbox_inches="tight", dpi=180)
            made.append(str(out))
        plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5))
    counts = df.groupby("year").size()
    counts.plot(ax=ax, color="#2d6a6a", lw=2)
    ax.set_title("Insider Filing Transactions by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Transactions")
    save_all(fig, "fig_sample_waterfall")

    if "code" in df.columns:
        heat = pd.crosstab(df["year"], df["code"]).tail(20)
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.heatmap(np.log1p(heat), cmap="viridis", ax=ax)
        ax.set_title("Transaction Code Mix by Year")
        ax.set_xlabel("Transaction Code")
        ax.set_ylabel("Year")
        save_all(fig, "fig_code_mix_heatmap")

    role_cols = [c for c in ["is_ceo_cfo", "is_director_role", "is_officer_role", "is_10b5", "has_derivative_companion"] if c in df.columns]
    if role_cols and "p_informative" in df.columns:
        role_stats = []
        for col in role_cols:
            mask = df[col].fillna(False).astype(bool)
            role_stats.append({"role_or_flag": col, "group": "true", "mean_p_info": df.loc[mask, "p_informative"].mean()})
            role_stats.append({"role_or_flag": col, "group": "false", "mean_p_info": df.loc[~mask, "p_informative"].mean()})
        role_heat = pd.DataFrame(role_stats).pivot(index="role_or_flag", columns="group", values="mean_p_info")
        fig, ax = plt.subplots(figsize=(7, 4))
        sns.heatmap(role_heat, annot=True, fmt=".3f", cmap="mako", ax=ax)
        ax.set_title("Role and Filing-Flag Signal Strength")
        ax.set_xlabel("")
        ax.set_ylabel("")
        save_all(fig, "fig_role_signal_heatmap")

    if panel_path.exists():
        panel = pd.read_parquet(panel_path)
        if "ym" in panel.columns and "liis_net" in panel.columns:
            ts = panel.groupby("ym")["liis_net"].sum()
            fig, ax = plt.subplots(figsize=(12, 5))
            ts.plot(ax=ax, color="#874c62", lw=1.5)
            ax.set_title("Aggregate LIIS Net Intensity")
            ax.set_xlabel("Month")
            ax.set_ylabel("Signed Informative Value")
            every = max(1, len(ts) // 12)
            ax.set_xticks(range(0, len(ts), every))
            ax.set_xticklabels(ts.index[::every], rotation=45, ha="right")
            save_all(fig, "fig_liis_timeseries")

    event_path = paths.tables / "event_car_by_decile.csv"
    if event_path.exists():
        event_car = pd.read_csv(event_path, index_col=0)
        fig, ax = plt.subplots(figsize=(10, 5))
        for dec in [1.0, 5.0, 10.0]:
            key = str(dec)
            if key in event_car.index.astype(str):
                row = event_car.loc[event_car.index.astype(str) == key].iloc[0]
                ax.plot([int(c) for c in row.index], row.values, lw=2, label=f"Decile {int(dec)}")
        ax.axvline(0, color="black", lw=1, ls="--")
        ax.set_title("Event-Time CAR Around Filing Availability")
        ax.set_xlabel("Business Days from Filing")
        ax.set_ylabel("Cumulative Return")
        ax.legend()
        save_all(fig, "fig_event_car_by_decile")

    decile_path = paths.tables / "decile_returns.csv"
    if decile_path.exists():
        dec = pd.read_csv(decile_path, index_col=0)
        numeric_cols = [c for c in dec.columns if re.fullmatch(r"\d+\.?0?", str(c))]
        if numeric_cols:
            means = dec[numeric_cols].mean().sort_index(key=lambda s: s.astype(float))
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.bar(range(1, len(means) + 1), means.values, color="#2d6a6a")
            ax.set_title("Next-Month Returns by LIIS Decile")
            ax.set_xlabel("LIIS Decile")
            ax.set_ylabel("Average Next-Month Return")
            ax.set_xticks(range(1, len(means) + 1))
            save_all(fig, "fig_decile_monotonicity")
        if "cum_long_short_10_1" in dec.columns:
            fig, ax = plt.subplots(figsize=(12, 5))
            dec["cum_long_short_10_1"].plot(ax=ax, color="#874c62", lw=1.8)
            ax.set_title("Cumulative Long-Short LIIS Return")
            ax.set_xlabel("Month")
            ax.set_ylabel("Cumulative Return")
            every = max(1, len(dec) // 12)
            ax.set_xticks(range(0, len(dec), every))
            ax.set_xticklabels(dec.index[::every], rotation=45, ha="right")
            save_all(fig, "fig_cumulative_pnl_gross_net")

    drawdown_path = paths.tables / "drawdown_turnover.csv"
    if drawdown_path.exists():
        dd = pd.read_csv(drawdown_path)
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(dd["ym"], dd["drawdown"], color="#874c62", lw=1.8)
        ax.set_title("Long-Short Drawdown")
        ax.set_xlabel("Month")
        ax.set_ylabel("Drawdown")
        every = max(1, len(dd) // 12)
        ax.set_xticks(range(0, len(dd), every))
        ax.set_xticklabels(dd["ym"].iloc[::every], rotation=45, ha="right")
        save_all(fig, "fig_drawdown_turnover")

    loadings_path = paths.tables / "factor_loadings.csv"
    if loadings_path.exists():
        load = pd.read_csv(loadings_path)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(load["term"], load["loading"], color="#2d6a6a")
        ax.axhline(0, color="black", lw=1)
        ax.set_title("Long-Short Factor Loadings")
        ax.set_xlabel("Factor")
        ax.set_ylabel("Loading")
        save_all(fig, "fig_factor_loadings")

    shap_path = paths.tables / "stage1_shap_summary.csv"
    if shap_path.exists():
        shap_df = pd.read_csv(shap_path).head(15)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(shap_df["feature"][::-1], shap_df["mean_abs_shap"][::-1], color="#2d6a6a")
        ax.set_title("Stage-One SHAP Summary")
        ax.set_xlabel("Mean Absolute SHAP")
        ax.set_ylabel("")
        save_all(fig, "fig_shap_summary")

    if "p_informative" in df.columns and len(df):
        top = df.nlargest(1, "p_informative").iloc[0]
        fig, ax = plt.subplots(figsize=(7, 4))
        fields = {
            "p_info": float(top.get("p_informative", np.nan)),
            "log_value": float(np.log1p(abs(top.get("dollar_value", 0) or 0))),
            "filing_lag": float(top.get("filing_lag_days", np.nan)) if pd.notna(top.get("filing_lag_days", np.nan)) else 0,
            "open_market": float(bool(top.get("is_open_market", False))),
            "mechanical": float(bool(top.get("is_mechanical_code", False))),
        }
        ax.bar(fields.keys(), fields.values(), color="#874c62")
        ax.set_title("Individual Filing Explanation Proxy")
        ax.tick_params(axis="x", rotation=30)
        save_all(fig, "fig_individual_filing_explanation")

    variant_path = paths.tables / "model_variant_table.csv"
    if variant_path.exists():
        variant = pd.read_csv(variant_path)
        fig, ax = plt.subplots(figsize=(9, 3))
        ax.axis("off")
        tbl = ax.table(cellText=variant.values, colLabels=variant.columns, loc="center", cellLoc="left")
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        tbl.scale(1, 1.4)
        fig.savefig(paths.figures_static / "fig_model_variant_table.png", bbox_inches="tight", dpi=180)
        made.append(str(paths.figures_static / "fig_model_variant_table.png"))
        plt.close(fig)

    flow = """flowchart LR
    A[Schema audit] --> B[WRDS extracts]
    B --> C[Issuer mapping and cleaning]
    C --> D[Transaction information model]
    D --> E[Firm-month LIIS panel]
    E --> F[Asset-pricing tests]
    F --> G[Figures and public-safe docs]
"""
    (paths.figures_static / "pipeline_flowchart.mmd").write_text(flow, encoding="utf-8")
    made.append(str(paths.figures_static / "pipeline_flowchart.mmd"))
    gantt = """gantt
    title Latent Insider Information Shocks timeline
    dateFormat  YYYY-MM-DD
    section Design
    Freeze hypotheses and repo scaffold      :done, a1, 2026-06-01, 7d
    Audit WRDS schema and field mapping      :done, a2, after a1, 7d
    section Data
    Extract insider CRSP and Compustat data  :done, b1, after a2, 10d
    Build point-in-time filing panel         :active, b2, after b1, 7d
    section Models
    Train stage-one information model        :active, c1, after b2, 10d
    Build firm-month factors and baselines   :active, c2, after c1, 7d
    Run main ML backtests and robustness     :c3, after c2, 14d
    section Delivery
    Create figures docs and release package  :d1, after c3, 10d
"""
    (paths.figures_static / "timeline_gantt.mmd").write_text(gantt, encoding="utf-8")
    made.append(str(paths.figures_static / "timeline_gantt.mmd"))
    manifest(paths, "figures", {"status": "ok", "files": made})


def run_phase(args, paths: Paths, cfg: dict) -> None:
    if args.phase in {"all", "schema", "extract"}:
        wrds_smoke_or_stop(paths, int(cfg["wrds"]["connect_timeout_seconds"]))

    schema = None
    if args.phase in {"all", "schema"}:
        schema = audit_schema(paths, cfg)
    elif args.phase in {"extract"}:
        schema = load_schema(paths)

    if args.phase in {"all", "extract"}:
        if schema is None:
            schema = load_schema(paths)
        run_extract(paths, cfg, schema, include_taq=args.include_taq)

    if args.phase in {"all", "features"}:
        build_features(paths)

    if args.phase in {"all", "models"}:
        train_models(paths, cfg)

    if args.phase in {"all", "backtest"}:
        run_backtests(paths)

    if args.phase in {"all", "figures"}:
        make_figures(paths)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="Project root")
    parser.add_argument(
        "--phase",
        default="all",
        choices=["all", "schema", "extract", "features", "models", "backtest", "figures"],
    )
    parser.add_argument("--include-taq", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    root = Path(args.root).resolve()
    paths = project_paths(root)
    ensure_dirs(paths)
    setup_logging(paths)
    cfg = load_config(root)
    logging.info("root=%s phase=%s include_taq=%s", root, args.phase, args.include_taq)
    try:
        run_phase(args, paths, cfg)
        manifest(paths, "pipeline_complete", {"status": "ok", "phase": args.phase})
        logging.info("pipeline complete")
        return 0
    except SystemExit:
        raise
    except Exception as exc:
        logging.error("pipeline failed: %s", exc)
        logging.error(traceback.format_exc())
        manifest(
            paths,
            "pipeline_failed",
            {
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
                "traceback": traceback.format_exc()[-4000:],
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
