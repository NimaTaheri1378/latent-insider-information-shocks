# Pipeline

The core pipeline lives in `scripts/liis_pipeline.py`.

Main phases:

1. `extract`: WRDS smoke, schema audit, insider support tables, CRSP,
   Compustat, CCM, daily CRSP, Fama/French, and TAQ entitlement probe.
2. `features`: filing-date-clean transaction features and firm-month
   aggregation.
3. `models`: rules baseline, Elastic Net, LightGBM, A100 PyTorch MLP, SHAP, and
   blended transaction scores.
4. `backtest`: PERMNO-month return joins, deciles, Fama-MacBeth-style tests,
   robustness, event-time CAR, factor loadings, drawdown, and performance.
5. `figures`: publication static figures and Mermaid diagrams.

Use `scripts/make_interactive_figures.py` to rebuild HTML companions from public
CSV outputs.
