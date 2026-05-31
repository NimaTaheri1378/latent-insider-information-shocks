# Proposal Coverage Ledger

This ledger maps the proposal requirements to implemented public-safe artifacts.
It excludes manuscript drafting, per the project instruction.

## Completed

| Proposal item | Evidence |
| --- | --- |
| WRDS smoke and schema discovery | `manifests/wrds_smoke.json`, `manifests/schema_audit.json` locally ignored; summarized in `docs/run_status.md` |
| Thomson Reuters insider Table 1 extraction | `manifests/extract_summary.json`; raw shards kept off Git |
| Derivative Table 2 and support tables | Table 2, header, 10b5, amendment, and company support shards completed on Amarel; raw shards kept off Git |
| Filing-date-clean transaction features | `manifests/feature_build.json`; processed panels kept off Git |
| CRSP, CCM, Compustat, and daily CRSP support | Completed in cached Amarel extract; raw shards kept off Git |
| Stage-one rules baseline | `artifacts/tables/model_variant_table.csv` |
| Elastic Net benchmark | `artifacts/tables/model_variant_table.csv`; status `ok` |
| LightGBM workhorse | `artifacts/tables/model_variant_table.csv`; status `ok_lightgbm_cpu` |
| GPU extension | A100 PyTorch MLP branch; status `ok`; summarized in `docs/run_status.md` |
| SHAP explainability | `artifacts/tables/stage1_shap_summary.csv`, `artifacts/figures_static/fig_shap_summary.*` |
| Firm-month LIIS panel | `manifests/backtest.json`; processed panel kept off Git |
| Decile and long-short tests | `artifacts/tables/decile_returns.csv`, `artifacts/tables/performance_summary.csv` |
| Fama-MacBeth-style tests | `artifacts/tables/fama_macbeth_liis.csv`, `artifacts/tables/fama_macbeth_summary.csv` |
| Factor loading table and figure | `artifacts/tables/factor_loadings.csv`, `artifacts/figures_static/fig_factor_loadings.*` |
| Event-time CAR | `artifacts/tables/event_car_by_decile.csv`, `artifacts/figures_static/fig_event_car_by_decile.*` |
| Robustness battery | `artifacts/tables/robustness_summary.csv`; 10 tests |
| Drawdown and performance | `artifacts/tables/drawdown_turnover.csv`, `artifacts/figures_static/fig_drawdown_turnover.*` |
| Required static visual pack | `artifacts/figures_static/`; 36 files in the final figure manifest |
| Mermaid flowchart and timeline | `artifacts/figures_static/pipeline_flowchart.mmd`, `artifacts/figures_static/timeline_gantt.mmd` |
| HTML companions | `artifacts/figures_html/` |
| Public docs site scaffolding | `mkdocs.yml`, `docs/`, `.github/workflows/pages.yml` |
| CI and reproducibility checks | `.github/workflows/ci.yml`, `tests/test_public_artifacts.py`, `scripts/public_safety_scan.py` |
| Public-safety guardrails | `.gitignore`, `.env.example`, `DATA_ACCESS.md`, `scripts/public_safety_scan.py` |

## Explicit Limitations

| Proposal item | Current status |
| --- | --- |
| LightGBM GPU tree learner | Attempted, but the installed LightGBM build lacks GPU tree support. The final score uses LightGBM CPU plus an A100 PyTorch GPU MLP blend. |
| TAQ-based cost calibration | TAQ schemas were visible, but sample access returned WRDS permission errors. The production outputs therefore use CRSP-based implementation and cost support. |
| SEC high-resolution acceptance-time appendix | Not run in the production pass. The current design remains filing-date clean; acceptance-time enrichment is optional for a later attention-mechanism appendix. |
| Manuscript | Intentionally not written. |

