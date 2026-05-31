# Data Access

The production run uses WRDS Thomson Reuters Insiders, CRSP, CRSP/Compustat
linking files, Compustat annual and quarterly data, and public Fama/French
factor files.

The pipeline discovers available insider schemas before pulling data, shards
large extracts by year, caches successful shards, and reruns downstream phases
from cached Parquet. TAQ is treated as an optional entitlement-dependent
appendix. SEC EDGAR metadata enrichment remains optional unless a precise
filing-timestamp appendix is required.

No proprietary row-level data is committed.
