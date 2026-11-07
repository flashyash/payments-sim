#!/usr/bin/env python3
"""Checks written after the main analysis, to see whether the result depends on a particular subset of rounds.
Not part of the decision rule in PROTOCOL.md. usage: sensitivity.py RAW_DIR"""
import json, random, sys
from pathlib import Path

S = ["16 shards, 20 merchants", "1 shard (hot accounts), 20 merchants", "16 shards, 1 merchant (hot pending)", "16 shards, 20 merchants, network 20–50 ms"]
d = {}
for p in Path(sys.argv[1]).glob("round*.json"):
    b = json.loads(p.read_text())
    for r in b["load"]["out"]:
        d[(b["round"], b["side"], r["scenario"])] = r
mean = lambda x: sum(x) / len(x)


def run(name, rounds, drop=frozenset()):
    print(name)
    for s in S:
        rs = [r for r in rounds if (r, s) not in drop]
        eff = lambda q: mean([d[(r, "new", s)]["flowsPerSec"] for r in q]) / mean([d[(r, "old", s)]["flowsPerSec"] for r in q]) - 1
        g = random.Random(20261106)
        o = sorted(eff([g.choice(rs) for _ in rs]) for _ in range(10000))
        print(f"  {s:44} n={len(rs):2} {eff(rs)*100:+.1f}% [{o[250]*100:+.1f}%, {o[9749]*100:+.1f}%]")


excluded = {(1, S[0]), (10, S[1])}
run("all 12 rounds, nothing excluded", range(1, 13))
run("rounds 1-9, with the protocol's exclusions", range(1, 10), excluded)
run("rounds 1-6, with the protocol's exclusions", range(1, 7), excluded)
run("rounds 7-12, with the protocol's exclusions", range(7, 13), excluded)
loads = [r["hostLoadAverage"]["before"] for r in d.values()]
print(f"host 1-min load average before each run: {min(loads)} to {max(loads)}")
