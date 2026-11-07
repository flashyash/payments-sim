#!/usr/bin/env python3
"""Runs the alternating old/new blocks described in PROTOCOL.md and saves every block's raw output.

usage: run.py OLD_WORKTREE NEW_WORKTREE LEDGER_DIR OUT_DIR [--rounds N] [--seconds S]
"""
import argparse, hashlib, json, os, re, shutil, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("old"); ap.add_argument("new"); ap.add_argument("ledger"); ap.add_argument("out")
ap.add_argument("--rounds", type=int, default=12)
ap.add_argument("--seconds", type=int, default=15)
args = ap.parse_args()

trees = {"old": Path(args.old), "new": Path(args.new)}
out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
IDLE_MIN = 75
IDLE_WAIT_S = 300
PAUSE_S = 20


def on_ac():
    return "AC Power" in subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True).stdout


def host_idle():
    o = subprocess.run(["top", "-l", "2", "-n", "0", "-s", "3"], capture_output=True, text=True).stdout
    m = re.findall(r"([0-9.]+)% idle", o)
    return float(m[-1]) if m else None


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def head(p):
    return subprocess.run(["git", "-C", str(p), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


if sha(trees["old"] / "scripts/load.ts") != sha(trees["new"] / "scripts/load.ts"):
    sys.exit("load.ts differs between sides")

caff = subprocess.Popen(["caffeinate", "-dims", "-w", str(os.getpid())])
log = open(out / "run.log", "a", buffering=1)


def say(s):
    line = f"{datetime.now().isoformat(timespec='seconds')} {s}"
    print(line, flush=True); log.write(line + "\n")


consecutive_failures = 0
for rnd in range(1, args.rounds + 1):
    order = ["old", "new"] if rnd % 2 == 1 else ["new", "old"]
    for side in order:
        dest = out / f"round{rnd:02d}-{side}.json"
        if dest.exists():
            continue
        waited = 0
        idle = host_idle()
        while (idle is None or idle < IDLE_MIN) and waited < IDLE_WAIT_S:
            say(f"round {rnd} {side}: host idle {idle}, waiting")
            time.sleep(15); waited += 15
            idle = host_idle()
        ac_start = on_ac()
        started = datetime.now(timezone.utc).isoformat()
        t0 = time.time()
        results = trees[side] / "results/load.json"
        results.unlink(missing_ok=True)
        env = {**os.environ, "LEDGER_DIR": args.ledger, "LOAD_SECONDS": str(args.seconds), "LOAD_RUNS": "1", "LOAD_CONCURRENCY": "20"}
        env.pop("LOAD_ONLY", None)
        try:
            p = subprocess.run(["node", "scripts/load.ts"], cwd=trees[side], env=env, capture_output=True, text=True, timeout=1500)
            code, stdout, stderr = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            code, stdout, stderr = "timeout", str(e.stdout or ""), str(e.stderr or "")
        data = json.loads(results.read_text()) if results.exists() else None
        block = {
            "round": rnd, "side": side, "position": order.index(side) + 1, "commit": head(trees[side]),
            "loadScriptSha256": sha(trees[side] / "scripts/load.ts"), "startedAt": started, "wallSeconds": round(time.time() - t0, 1),
            "preIdlePercent": idle, "idleWaitSeconds": waited, "acAtStart": ac_start, "acAtEnd": on_ac(),
            "exitCode": code, "stderr": stderr[-2000:], "stdout": stdout, "load": data,
        }
        dest.write_text(json.dumps(block, indent=1))
        n = len(data["out"]) if data else 0
        say(f"round {rnd} {side}: exit {code}, {n} scenarios, {block['wallSeconds']} s, pre-idle {idle}, ac {ac_start}/{block['acAtEnd']}")
        consecutive_failures = consecutive_failures + 1 if code != 0 or n != 4 else 0
        if consecutive_failures >= 3:
            say("stopping: 3 failed blocks in a row")
            caff.terminate(); sys.exit(2)
        time.sleep(PAUSE_S)
say("done")
caff.terminate()
