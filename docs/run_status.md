# Run Status

Last updated: 2026-05-31.

The max-proposal Amarel run completed inside the project allocation on
`gpun001` using the exact project root:

`/scratch/nt612/Github/Latent Insider Information Shocks/`

Completed public-safe outputs:

- WRDS smoke and schema audit
- Thomson Reuters Insiders extraction: Table 1, derivative Table 2, headers,
  10b5, amendment, and company support tables
- CRSP monthly and daily extraction
- CRSP names and CCM link extraction
- Compustat annual and quarterly extraction
- Fama/French public factor download attempt
- TAQ entitlement probe and sample attempt
- transaction-level weak-supervision model
- rules-only, Elastic Net, LightGBM, and A100 PyTorch GPU MLP model variants
- LightGBM CPU plus A100 PyTorch GPU MLP blended scoring
- PERMNO-month LIIS panel with CRSP forward returns
- decile returns, cumulative long-short return, factor loadings, event-time
  CAR, robustness battery, and Fama-MacBeth-style summary
- static figures in PDF, SVG, and PNG

Important limitations from the run:

- LightGBM on this Amarel environment does not have GPU tree learner support, so
  LightGBM ran on CPU.
- GPU modeling was still used through a PyTorch MLP branch on the A100.
- TAQ schemas were visible but several sample queries returned WRDS permission
  errors, so the first pass uses CRSP-based implementation/cost support.
- No raw WRDS, CRSP, Compustat, TAQ, or row-level proprietary data should be
  committed.

Key public-safe summary metrics:

- transaction-score rows: 17,328,988
- mapped transaction rows in PERMNO backtest: 11,624,607
- PERMNO-month LIIS panel rows: 390,627
- backtest months: 203
- Fama-MacBeth-style months: 203
- robustness tests: 10
- event-window observations: 939,844 across 10 deciles
- Elastic Net status: ok
- SHAP summary status: ok
- final score source: LightGBM CPU plus torch GPU MLP blend
- A100 PyTorch branch: 3,000,000 training rows, 3 epochs
- figure manifest: 36 files
- HTML companions: cumulative LIIS return, event-time CAR, robustness dashboard
- delivery scaffolding: MkDocs pages, GitHub Actions CI, GitHub Pages workflow,
  and public artifact tests

Verification:

- `python scripts/public_safety_scan.py`: passed
- `python -m py_compile scripts/liis_pipeline.py scripts/public_safety_scan.py`: passed
- `python -m py_compile scripts/make_interactive_figures.py scripts/build_docs_assets.py`: passed
- `python -m unittest discover -s tests`: passed
- `python scripts/make_interactive_figures.py`: passed
- `python scripts/build_docs_assets.py`: passed
- public-safe logs, manifests, tables, and static figures were downloaded into
  the local workspace
