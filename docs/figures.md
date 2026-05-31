# Figures

Static figures are stored in `artifacts/figures_static` as PDF, SVG, and PNG
where applicable. The visual pack includes sample construction, code-mix and
role heatmaps, LIIS time series, event-time CAR, decile monotonicity,
cumulative long-short returns, drawdowns, factor loadings, SHAP summary, and a
single-filing explanation figure.

HTML companions are stored in `artifacts/figures_html`:

- `cumulative_liis_return.html`
- `event_car_by_decile.html`
- `robustness_dashboard.html`

For GitHub Pages builds, `scripts/build_docs_assets.py` copies these public
artifacts into the MkDocs tree.
