#!/usr/bin/env python3
"""Applies the PROTOCOL.md exclusion rules to the raw blocks and prints the comparison. usage: analyze.py RAW_DIR"""
import json, random, sys
from collections import Counter, defaultdict
from pathlib import Path

raw = Path(sys.argv[1])
blocks = [json.loads(p.read_text()) for p in sorted(raw.glob("round*.json"))]
SCENARIOS = ["16 shards, 20 merchants", "1 shard (hot accounts), 20 merchants", "16 shards, 1 merchant (hot pending)", "16 shards, 20 merchants, network 20–50 ms"]


def reasons(b, r):
    why = []
    if r is None or "error" in r:
        return ["error"]
    if not r.get("valid"): why.append("invalid")
    if r.get("maxPauseMs", 0) >= 1000: why.append("pause")
    if r.get("failed", 0) > 0 or r.get("violations", 0) > 0: why.append("failed/violations")
    if r.get("hostIdlePercent") is None or r["hostIdlePercent"] < 40: why.append("host busy during")
    if b["preIdlePercent"] is None or b["preIdlePercent"] < 75: why.append("host busy before")
    if not (b["acAtStart"] and b["acAtEnd"]): why.append("not on AC")
    return why


vals = defaultdict(dict)  # scenario -> side -> round -> flows/s
excl = Counter(); excl_rule = Counter(); total = Counter(); violations = 0
for b in blocks:
    runs = {r["scenario"]: r for r in (b["load"] or {}).get("out", [])}
    for sc in SCENARIOS:
        r = runs.get(sc)
        total[b["side"]] += 1
        violations += (r or {}).get("violations", 0)
        why = reasons(b, r)
        if why:
            excl[b["side"]] += 1
            for w in why: excl_rule[(b["side"], w)] += 1
            print(f"excluded: round {b['round']} {b['side']} {sc}: {', '.join(why)}")
        else:
            vals[sc].setdefault(b["side"], {})[b["round"]] = r["flowsPerSec"]

mean = lambda xs: sum(xs) / len(xs)
print(f"\nblocks {len(blocks)}; scenario runs {dict(total)}; excluded {dict(excl)}; by rule {dict(excl_rule)}; invariant violations {violations}\n")


def boot(rounds, stat, n=10000):
    rng = random.Random(20261106)
    out = sorted(stat([rng.choice(rounds) for _ in rounds]) for _ in range(n))
    return out[int(0.025 * n)], out[int(0.975 * n) - 1]


per = {}
print(f"{'scenario':44} {'old mean (range) n':24} {'new mean (range) n':24} pairs  effect   95% CI")
for sc in SCENARIOS:
    o, nw = vals[sc].get("old", {}), vals[sc].get("new", {})
    pairs = sorted(set(o) & set(nw))
    per[sc] = (o, nw, pairs)
    if not pairs:
        print(sc, "no valid pairs"); continue
    eff = lambda rs: mean([nw[r] for r in rs]) / mean([o[r] for r in rs]) - 1
    lo, hi = boot(pairs, eff)
    f = lambda d: f"{mean(list(d.values())):.1f} ({min(d.values())}-{max(d.values())}) {len(d)}"
    print(f"{sc:44} {f(o):24} {f(nw):24} {len(pairs):5}  {eff(pairs)*100:+5.1f}%  [{lo*100:+.1f}%, {hi*100:+.1f}%]")
    per[sc] = (o, nw, pairs, lo, hi)

common = sorted(set.intersection(*[set(p[2]) for p in per.values()])) if all(len(p) == 5 for p in per.values()) else []
if common:
    ov = lambda rs: mean([mean([per[s][1][r] for r in rs]) / mean([per[s][0][r] for r in rs]) - 1 for s in SCENARIOS])
    lo, hi = boot(common, ov)
    print(f"\nacross scenarios (mean of the four effects, {len(common)} rounds valid in all four): {ov(common)*100:+.1f}% [{lo*100:+.1f}%, {hi*100:+.1f}%]")

rows = [p for p in per.values() if len(p) == 5]
if len(rows) < 4 or any(len(p[2]) < 10 for p in rows): verdict = "NOT SETTLED: fewer than 10 valid pairs in a scenario"
elif any(p[4] < 0 for p in rows): verdict = "REGRESSION in at least one scenario"
elif all(p[3] > 0 for p in rows): verdict = "HOLDS: every scenario CI is above zero"
else: verdict = "NOT SETTLED: a scenario CI includes zero"
print("\nverdict:", verdict)

print("\ncontext, medians per side:")
for side in ("old", "new"):
    rs = [r for b in blocks if b["side"] == side for r in (b["load"] or {}).get("out", []) if "error" not in r]
    med = lambda f: sorted(f(r) for r in rs)[len(rs) // 2] if rs else None
    print(f"  {side}: commits/flow {med(lambda r: r['commitsPerFlow'])}, flow p50 {med(lambda r: r['flowMs']['p50'])} ms, flow p99 {med(lambda r: r['flowMs']['p99'])} ms, host idle {med(lambda r: r.get('hostIdlePercent') or 0)}%, vm idle {med(lambda r: r.get('vmIdlePercent') or 0)}%, pg cpu {med(lambda r: r['postgresCpuPercent'])}%")
pos = defaultdict(list)
for sc in SCENARIOS:
    if len(per[sc]) == 5:
        o, nw, pairs, *_ = per[sc]
        for r in pairs: pos["old first" if r % 2 else "new first"].append(nw[r] / o[r] - 1)
print("  order check, mean effect by which side ran first:", {k: f"{mean(v)*100:+.1f}%" for k, v in pos.items()})
