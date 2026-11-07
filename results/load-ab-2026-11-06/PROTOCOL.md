# Old-vs-new load comparison: protocol

Written before any run of this comparison. Run date: 2026-11-06.

## Question
Did the query changes in 25fbe99 (prepared statements, single fenced statements for step updates, less frequent lease
renewal) raise closed-loop throughput?

## Sides
- old: cd5305a, the commit before the change.
- new: f9c1d45, main at the time of the run. It has the same code as 25fbe99; f9c1d45 only edits the README.
- 25fbe99 also removed the sabotage checks from `src/` and added a rewrite hook to the fault proxy. So this compares the
  commits as a whole and can't attribute the result to any single query change.

## Same measurement on both sides
- Both sides run the same `scripts/load.ts` (the f9c1d45 version, copied over the old worktree). The cd5305a version has
  no VM and host sampling during the measured window, so running each side's own script would measure different things.
- Each side has its own temporary git worktree with `npm ci` from the same lockfile.
- Both use the same ledger checkout (a704449) through LEDGER_DIR and the same Postgres container.
- Environment: Colima, 2 vCPUs, 4 GB, ssh port-forward, postgres:17 from docker-compose.yml with durable commits.
- LOAD_SECONDS=15, LOAD_CONCURRENCY=20, LOAD_RUNS=1, all four scenarios in the script's order. Stack databases are wiped
  and recreated for every scenario by `Stack.start`.

## Schedule
12 rounds, each with one old block and one new block (one block = all four scenarios), 24 blocks in all. Odd rounds run
old then new, even rounds run new then old, so neither side always goes first. 20 s pause between blocks.

## Exclusion rules (fixed before looking at results)
A scenario run is excluded if any of these is true:
1. It threw an error, or the script reports `valid: false`.
2. `maxPauseMs` >= 1000 (the machine or VM froze).
3. `failed` > 0 or `violations` > 0. A violation is also a correctness alarm, reported whatever the throughput does.
4. Host busy during the run: `hostIdlePercent` is null or < 40. Earlier runs under this load read 40-64.
5. Host busy before the block: idle < 75% in the check before it. The runner waits up to 5 minutes for the host to go
   quiet, then runs and flags the block.
6. The laptop was not on AC power at the start or end of the block.

Exclusion counts are reported per side and per rule. Excluded runs stay in the raw results.

## Analysis
For each scenario, and for the mean across scenarios:
- Mean and range of flows/s per side, over non-excluded runs.
- Effect: new mean / old mean - 1, on rounds where both sides' runs survived exclusion.
- 95% percentile bootstrap CI for the effect, resampling rounds (10,000 resamples, seed 20261106).

## Decision rule
- Holds: all four scenario CIs are entirely above zero, with at least 10 valid pairs in each scenario.
- Not settled: any scenario CI includes zero, or results are mixed, or fewer than 10 valid pairs remain in a scenario.
- Regression: any scenario CI is entirely below zero. Reported as such.

## Scope of any claim
Single-laptop numbers under a saturated closed-loop test. They compare two commits on this setup and are not a capacity
claim.
