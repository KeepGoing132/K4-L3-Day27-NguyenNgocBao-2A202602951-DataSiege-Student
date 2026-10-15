"""Streaming checks using only the published API and observations from this run."""
from api import Verdict
import math


def register(ctx):
    for event, handler in (
        ("data_batch", check_data_batch),
        ("contract_checkpoint", check_contract_checkpoint),
        ("lineage_run", check_lineage_run),
        ("feature_materialization", check_feature_materialization),
        ("embedding_batch", check_embedding_batch),
    ):
        ctx.on(event, handler)


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _median(values):
    ordered = sorted(values)
    n = len(ordered)
    return (ordered[n // 2] + ordered[(n - 1) // 2]) / 2


def _group(payload):
    for field in ("feature_view", "corpus", "table", "dataset", "transform", "job", "pipeline"):
        if field in payload:
            return str(payload[field])
    return "default"


def _fetch(ctx, method, *args):
    cache = ctx.state.setdefault("tool_cache", {})
    key = (method,) + args
    if key not in cache:
        cache[key] = getattr(ctx.tools, method)(*args)
    return cache[key]


def _verdict(pillar, reasons):
    return Verdict(alert=bool(reasons), pillar=pillar,
                   reason="; ".join(reasons) if reasons else "Observed metrics within limits")


def _unavailable(pillar, result):
    return Verdict(alert=False, confidence=0.0, pillar=pillar,
                   reason="Measurement unavailable: " + str(result.get("error", "invalid response")))


def _upper(ctx, key, value, limit):
    """Published limit first; robust online calibration after 16 observations.

    MAD resists isolated faults. A scale floor and a 3.5-sigma cutoff keep
    small histories from producing overly narrow thresholds.
    """
    if not _number(value) or not _number(limit) or limit <= 0:
        return False
    histories = ctx.state.setdefault("metric_history", {})
    history = histories.setdefault(key, [])
    threshold = limit
    if len(history) >= 16:
        center = _median(history)
        scale = max(1.4826 * _median([abs(x - center) for x in history]), limit * 0.05)
        threshold = min(limit, center + 3.5 * scale)
    unusual = value > threshold
    # Exclude clear faults; retain small deviations so learning is not biased
    # by the adaptive threshold itself. The window also allows gradual change.
    if 0 <= value <= limit:
        history.append(value)
        if len(history) > 128:
            del history[0]
    return unusual


def _data_reasons(result, payload, ctx):
    baseline = ctx.baseline
    reasons = []
    deviations = []
    for field, low_key, high_key in (
        ("row_count", "row_count_min", "row_count_max"),
        ("mean_amount", "mean_amount_min", "mean_amount_max"),
    ):
        value = result.get(field)
        low, high = baseline[low_key], baseline[high_key]
        if _number(value):
            if not low <= value <= high:
                reasons.append(field + " outside calibrated interval")
            if high > low:
                deviations.append(abs(value - (low + high) / 2) / ((high - low) / 6))
    # Two moderately unusual, independent signals can jointly expose a shift
    # that remains inside each marginal three-sigma interval.
    if len(deviations) == 2 and min(deviations) > 2 and sum(z * z for z in deviations) > 12:
        reasons.append("joint volume and amount deviation")
    nulls = result.get("null_rate", {})
    customer_nulls = nulls.get("customer_id") if isinstance(nulls, dict) else None
    for name, value, bound in (
        ("customer_id null rate", customer_nulls, "null_rate_max"),
        ("batch staleness", result.get("staleness_min"), "staleness_min_max"),
    ):
        if _upper(ctx, ("checks", _group(payload), name), value, baseline[bound]):
            reasons.append(name + " above limit")
    return reasons


def check_data_batch(payload, ctx):
    result = _fetch(ctx, "batch_profile", payload["batch_id"])
    if not isinstance(result, dict) or "error" in result:
        return _unavailable("checks", result if isinstance(result, dict) else {})
    return _verdict("checks", _data_reasons(result, payload, ctx))


def check_contract_checkpoint(payload, ctx):
    result = _fetch(ctx, "contract_diff", payload["contract_id"], payload["checkpoint_batch_id"])
    if not isinstance(result, dict) or "error" in result:
        return _unavailable("contracts", result if isinstance(result, dict) else {})
    reasons = [str(v) for v in result.get("violations", [])]
    if _upper(ctx, ("contracts", payload["contract_id"], "freshness"),
              result.get("freshness_delay_min"), ctx.baseline["freshness_delay_max_min"]):
        reasons.append("contract freshness SLA exceeded")
    return _verdict("contracts", reasons)


def _topology(ctx, key, actual, expected):
    if expected is not None:
        return actual != expected
    # Where no declared topology is supplied, learn its mode per transform.
    # Require a stable majority before treating a different topology as faulty.
    counts = ctx.state.setdefault("topology_history", {}).setdefault(key, {})
    total = sum(counts.values())
    mode = max(counts, key=counts.get) if counts else actual
    unusual = total >= 8 and counts[mode] >= 0.75 * total and actual != mode
    counts[actual] = counts.get(actual, 0) + 1
    return unusual


def check_lineage_run(payload, ctx):
    result = _fetch(ctx, "lineage_graph_slice", payload["run_id"])
    if not isinstance(result, dict) or "error" in result:
        return _unavailable("lineage", result if isinstance(result, dict) else {})
    reasons = []
    group = _group(payload)
    upstream = result.get("actual_upstream")
    if isinstance(upstream, (list, tuple, set)):
        actual = tuple(sorted(set(upstream)))
        expected = payload.get("expected_upstream", payload.get("declared_upstream"))
        expected = tuple(sorted(set(expected))) if isinstance(expected, (list, tuple, set)) else None
        if _topology(ctx, (group, "upstream"), actual, expected):
            reasons.append("upstream edges differ from declared or stable topology")
    downstream = result.get("actual_downstream_count")
    if _number(downstream):
        if _topology(ctx, (group, "downstream"), downstream, payload.get("expected_downstream_count")):
            reasons.append("downstream outputs differ from declared or stable topology")
    if _upper(ctx, ("lineage", group, "duration"), result.get("duration_ms"),
              ctx.baseline["lineage_duration_ms_max"]):
        reasons.append("lineage runtime above limit")
    return _verdict("lineage", reasons)


def check_feature_materialization(payload, ctx):
    result = _fetch(ctx, "feature_drift", payload["feature_view"], payload["batch_id"])
    if not isinstance(result, dict) or "error" in result:
        return _unavailable("ai_infra", result if isinstance(result, dict) else {})
    shift = result.get("mean_shift_sigma")
    reasons = []
    if _upper(ctx, ("feature", payload["feature_view"]), shift,
              ctx.baseline["feature_mean_shift_sigma_max"]):
        reasons.append("training-serving feature skew")
    return _verdict("ai_infra", reasons)


def check_embedding_batch(payload, ctx):
    result = _fetch(ctx, "embedding_drift", payload["corpus"], payload["chunk_batch_id"])
    if not isinstance(result, dict) or "error" in result:
        return _unavailable("ai_infra", result if isinstance(result, dict) else {})
    reasons = []
    for field, bound in (("centroid_shift", "embedding_centroid_shift_max"),
                         ("avg_doc_age_days", "corpus_avg_doc_age_days_max")):
        if _upper(ctx, ("embedding", payload["corpus"], field), result.get(field), ctx.baseline[bound]):
            reasons.append(field + " above limit")
    return _verdict("ai_infra", reasons)
