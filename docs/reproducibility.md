# Reproducibility

The public repo is designed to be safe to clone without WRDS credentials. The
full production pipeline requires an authenticated WRDS environment and should
run on Amarel compute nodes, not login nodes.

Recommended checks before publishing:

```bash
python -m py_compile scripts/liis_pipeline.py scripts/public_safety_scan.py scripts/make_interactive_figures.py scripts/build_docs_assets.py
python scripts/public_safety_scan.py
python -m unittest discover -s tests
python scripts/make_interactive_figures.py
```

`logs/` and `manifests/` are retained locally for audit but ignored by Git.
