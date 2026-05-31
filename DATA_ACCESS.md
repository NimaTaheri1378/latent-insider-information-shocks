# Data Access

This project requires user-provided access to WRDS datasets and public-data APIs.

Do not commit credentials, API keys, `.pgpass`, raw WRDS extracts, CRSP data,
Compustat data, TAQ data, Thomson Reuters Insiders records, or derived row-level
proprietary panels.

Required WRDS datasets:

- Thomson Reuters Insiders, discovered by schema audit
- CRSP daily and monthly stock files
- CRSP names/history
- CRSP/Compustat Merged link history
- Compustat annual and quarterly fundamentals

Optional WRDS dataset:

- TAQ, for the liquidity and transaction-cost appendix

Public sources:

- Kenneth French factors
- SEC EDGAR metadata, using `SEC_USER_AGENT` from the private runtime
  environment

The repository should contain code, docs, synthetic fixtures, public-safe
figures, sanitized summary tables, and sanitized manifests only.
