# Results

The final public-safe run scored 17,328,988 insider transactions and mapped
11,624,607 transactions into the PERMNO-month backtest panel. The resulting LIIS
panel contains 390,627 PERMNO-month rows over 203 monthly backtest periods.

Key public tables:

- `artifacts/tables/model_variant_table.csv`
- `artifacts/tables/robustness_summary.csv`
- `artifacts/tables/performance_summary.csv`
- `artifacts/tables/event_car_by_decile.csv`
- `artifacts/tables/factor_loadings.csv`
- `artifacts/tables/fama_macbeth_summary.csv`

The final score source is a LightGBM CPU plus torch GPU MLP blend. LightGBM GPU
training was attempted but the installed LightGBM build did not include GPU tree
learner support; GPU modeling is therefore represented by the A100 PyTorch MLP
branch.
