"""Compare the signed practice report with the published practice labels."""
import json
from pathlib import Path


def compare(report, answers):
    verdicts = report["result"]["verdicts"]
    observed = {v["seq"]: v for v in verdicts}
    expected = {a["seq"]: a for a in answers}
    if len(observed) != len(verdicts) or len(expected) != len(answers):
        raise ValueError("Duplicate sequence numbers")
    if observed.keys() != expected.keys():
        raise ValueError("Verdicts and labels must cover exactly the same events")
    counts = dict(TP=0, FP=0, FN=0, TN=0)
    mismatches = []
    for seq, answer in expected.items():
        alert = observed[seq]["alert"]
        actual = answer["is_faulty"]
        if not isinstance(alert, bool) or not isinstance(actual, bool):
            raise ValueError("Alert and ground-truth labels must be booleans")
        category = ("TP" if alert else "FN") if actual else ("FP" if alert else "TN")
        counts[category] += 1
        if alert != actual:
            mismatches.append({"seq": seq, "category": category,
                               "fault_key": answer.get("fault_key"),
                               "pillar": answer.get("pillar")})
    return {"event_count": len(verdicts), **counts,
            "TPR": counts["TP"] / max(1, counts["TP"] + counts["FN"]),
            "FPR": counts["FP"] / max(1, counts["FP"] + counts["TN"]),
            "mismatches": mismatches}


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    report = json.loads((root / "solution/practice_report.json").read_text(encoding="utf-8-sig"))
    answers = json.loads((root / "phases/practice_answer_key.json").read_text(encoding="utf-8-sig"))
    summary = compare(report, answers)
    encoded = json.dumps(summary, indent=2) + "\n"
    (root / "solution/practice_comparison.json").write_text(encoded, encoding="utf-8")
    print(encoded, end="")
