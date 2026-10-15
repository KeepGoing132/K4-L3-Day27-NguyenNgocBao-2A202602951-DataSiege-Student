"""Local regression tests with synthetic observations, never phase secrets."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "harness" / "child_env"))
sys.path.insert(0, str(ROOT / "solution"))
import defense


class Tools:
    def __init__(self, **results):
        self.results = results
        self.calls = []

    def _get(self, method, args):
        self.calls.append((method, args))
        return self.results[method]

    def batch_profile(self, *args):
        return self._get("batch_profile", args)

    def contract_diff(self, *args):
        return self._get("contract_diff", args)

    def lineage_graph_slice(self, *args):
        return self._get("lineage_graph_slice", args)

    def feature_drift(self, *args):
        return self._get("feature_drift", args)

    def embedding_drift(self, *args):
        return self._get("embedding_drift", args)


def context(**results):
    baseline = json.loads((ROOT / "data" / "baselines.json").read_text(encoding="utf-8"))
    return SimpleNamespace(baseline=baseline, state={}, tools=Tools(**results))


class DetectionTests(unittest.TestCase):
    def test_clean_observations_and_all_handlers(self):
        ctx = context(
            batch_profile={"row_count": 500, "mean_amount": 82, "null_rate": {"customer_id": .002}, "staleness_min": 3},
            contract_diff={"violations": [], "freshness_delay_min": 4},
            lineage_graph_slice={"actual_upstream": ["orders", "customers"], "actual_downstream_count": 2, "duration_ms": 3000},
            feature_drift={"mean_shift_sigma": .1},
            embedding_drift={"centroid_shift": .01, "avg_doc_age_days": 15},
        )
        events = [
            (defense.check_data_batch, {"batch_id": "batch"}),
            (defense.check_contract_checkpoint, {"contract_id": "orders", "checkpoint_batch_id": "checkpoint"}),
            (defense.check_lineage_run, {"run_id": "run", "expected_upstream": ["customers", "orders"], "expected_downstream_count": 2}),
            (defense.check_feature_materialization, {"feature_view": "sales", "batch_id": "features"}),
            (defense.check_embedding_batch, {"corpus": "support", "chunk_batch_id": "chunks"}),
        ]
        for handler, payload in events:
            self.assertFalse(handler(payload, ctx).alert)
        self.assertEqual(len(ctx.tools.calls), 5)

    def test_each_data_fault_and_joint_moderate_deviation(self):
        clean = {"row_count": 500, "mean_amount": 82, "null_rate": {"customer_id": .002}, "staleness_min": 3}
        for field, value in [("row_count", 100), ("mean_amount", 150), ("null_rate", {"customer_id": .05}), ("staleness_min", 20)]:
            ctx = context(batch_profile=dict(clean, **{field: value}))
            self.assertTrue(defense.check_data_batch({"batch_id": "b"}, ctx).alert)
        ctx = context()
        b = ctx.baseline
        shifted = dict(clean)
        for field, low, high in [("row_count", "row_count_min", "row_count_max"), ("mean_amount", "mean_amount_min", "mean_amount_max")]:
            shifted[field] = (b[low] + b[high]) / 2 + 2.6 * (b[high] - b[low]) / 6
        ctx.tools.results["batch_profile"] = shifted
        self.assertTrue(defense.check_data_batch({"batch_id": "b"}, ctx).alert)

    def test_contract_schema_and_sla(self):
        for result in [{"violations": ["schema_hash_mismatch"], "freshness_delay_min": 1},
                       {"violations": [], "freshness_delay_min": 30}]:
            ctx = context(contract_diff=result)
            self.assertTrue(defense.check_contract_checkpoint({"contract_id": "c", "checkpoint_batch_id": "b"}, ctx).alert)

    def test_lineage_declared_topology_and_legitimate_sink(self):
        ctx = context(lineage_graph_slice={"actual_upstream": ["orders"], "actual_downstream_count": 0, "duration_ms": 2000})
        self.assertTrue(defense.check_lineage_run({"run_id": "r", "expected_upstream": ["orders", "customers"]}, ctx).alert)
        self.assertFalse(defense.check_lineage_run({"run_id": "r", "expected_upstream": ["orders"], "expected_downstream_count": 0}, ctx).alert)

    def test_topology_learns_mode_and_isolates_transforms(self):
        ctx = context()
        for _ in range(10):
            self.assertFalse(defense._topology(ctx, ("job-a", "upstream"), ("a", "b"), None))
        self.assertTrue(defense._topology(ctx, ("job-a", "upstream"), ("a",), None))
        self.assertFalse(defense._topology(ctx, ("job-b", "upstream"), ("x",), None))

    def test_feature_and_embedding_faults(self):
        ctx = context(feature_drift={"mean_shift_sigma": 1})
        self.assertTrue(defense.check_feature_materialization({"feature_view": "f", "batch_id": "b"}, ctx).alert)
        for result in [{"centroid_shift": .2, "avg_doc_age_days": 10}, {"centroid_shift": .01, "avg_doc_age_days": 100}]:
            ctx = context(embedding_drift=result)
            self.assertTrue(defense.check_embedding_batch({"corpus": "c", "chunk_batch_id": "b"}, ctx).alert)

    def test_history_resists_outliers_then_detects_subtle_shift(self):
        ctx = context()
        for i in range(20):
            self.assertFalse(defense._upper(ctx, "metric", 19 + i % 3, 100))
        self.assertTrue(defense._upper(ctx, "metric", 500, 100))
        self.assertTrue(defense._upper(ctx, "metric", 50, 100))
        self.assertFalse(defense._upper(ctx, "metric", 20, 100))

    def test_cache_and_unavailable_measurement(self):
        ctx = context(feature_drift={"error": "unknown or not-yet-visible feature batch"})
        payload = {"feature_view": "f", "batch_id": "b"}
        for _ in range(2):
            verdict = defense.check_feature_materialization(payload, ctx)
            self.assertFalse(verdict.alert)
            self.assertEqual(verdict.confidence, 0)
        self.assertEqual(len(ctx.tools.calls), 1)


if __name__ == "__main__":
    unittest.main()
