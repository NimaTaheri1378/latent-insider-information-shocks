from __future__ import annotations

import csv
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PublicArtifactTests(unittest.TestCase):
    def test_no_blocked_tracked_runtime_paths(self) -> None:
        result = subprocess.run(["git", "ls-files"], cwd=ROOT, check=True, capture_output=True, text=True)
        tracked = [line.replace("\\", "/") for line in result.stdout.splitlines()]
        blocked_parts = ("artifacts/raw/", "artifacts/processed/", "artifacts/models/", "logs/", "manifests/")
        blocked_suffixes = (".parquet", ".feather", ".h5", ".hdf5", ".pkl", ".pickle", ".sqlite", ".db")
        bad = [path for path in tracked if path.endswith(blocked_suffixes) or any(part in path for part in blocked_parts)]
        self.assertEqual([], bad)

    def test_required_public_tables_exist(self) -> None:
        required = [
            "decile_returns.csv",
            "event_car_by_decile.csv",
            "factor_loadings.csv",
            "fama_macbeth_summary.csv",
            "model_variant_table.csv",
            "performance_summary.csv",
            "robustness_summary.csv",
            "stage1_shap_summary.csv",
        ]
        missing = [name for name in required if not (ROOT / "artifacts" / "tables" / name).exists()]
        self.assertEqual([], missing)

    def test_model_variant_table_records_gpu_branch(self) -> None:
        path = ROOT / "artifacts" / "tables" / "model_variant_table.csv"
        with path.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        status_by_model = {row["model"]: row["status"] for row in rows}
        self.assertEqual("ok", status_by_model.get("elastic_net_logistic"))
        self.assertEqual("ok", status_by_model.get("torch_gpu_mlp"))
        self.assertEqual("lightgbm_cpu_torch_gpu_mlp_blend", status_by_model.get("blend"))

    def test_required_static_figures_exist(self) -> None:
        names = [
            "fig_sample_waterfall",
            "fig_code_mix_heatmap",
            "fig_role_signal_heatmap",
            "fig_liis_timeseries",
            "fig_event_car_by_decile",
            "fig_decile_monotonicity",
            "fig_cumulative_pnl_gross_net",
            "fig_drawdown_turnover",
            "fig_factor_loadings",
            "fig_shap_summary",
            "fig_individual_filing_explanation",
        ]
        missing = []
        for stem in names:
            for suffix in (".pdf", ".svg", ".png"):
                if not (ROOT / "artifacts" / "figures_static" / f"{stem}{suffix}").exists():
                    missing.append(f"{stem}{suffix}")
        self.assertEqual([], missing)

    def test_html_companions_exist(self) -> None:
        required = [
            "cumulative_liis_return.html",
            "event_car_by_decile.html",
            "robustness_dashboard.html",
        ]
        missing = [name for name in required if not (ROOT / "artifacts" / "figures_html" / name).exists()]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
