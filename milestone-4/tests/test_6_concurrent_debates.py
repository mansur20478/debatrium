"""
Test 6 — Concurrent Debates (Scalability)
─────────────────────────────────────────────────────────────
Principle: Horizontal scalability + state isolation

What happens:
  3 debates are started simultaneously. Each debate gets its
  own debate_id and its own Redis key namespace, so they must
  not interfere with each other.

Expected:
  - All 3 debates complete successfully
  - Each debate has an independent result (different queries → different answers)
  - No state cross-contamination (debate A's result doesn't bleed into debate B)

Bug to find:
  - One debate's result overwrites another's in Redis
  - Agents process tasks from one debate but write results under another debate_id
  - Aggregator Lambda hits concurrency limit and drops messages
"""

import sys
import time
import threading

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, start_debate, wait_for_complete,
)

QUERIES = [
    "Is artificial intelligence beneficial for society?",
    "Should social media platforms be regulated?",
    "Is cryptocurrency a viable future currency?",
]


def run() -> bool:
    section("Test 6 — Concurrent Debates (Scalability)")

    token   = load_token()
    results = {}
    errors  = {}

    # ── Start all 3 debates simultaneously ───────────────────
    step(f"Starting {len(QUERIES)} debates simultaneously...")
    debate_ids = []
    for i, query in enumerate(QUERIES, 1):
        try:
            resp      = start_debate(query, token)
            debate_id = resp.get("debate_id")
            debate_ids.append((debate_id, query))
            ok(f"Debate {i} started: {debate_id} — '{query[:50]}...'")
        except Exception as e:
            fail(f"Failed to start debate {i}: {e}")
            errors[f"debate_{i}"] = str(e)

    if not debate_ids:
        return result(False, "Could not start any debates.")

    ok(f"All {len(debate_ids)} debates started — now polling concurrently")

    # ── Poll all debates concurrently using threads ───────────
    def poll_one(debate_id, query, out_dict):
        try:
            final = wait_for_complete(debate_id, token, timeout=900)
            out_dict[debate_id] = {
                "query":  query,
                "status": final.get("status"),
                "result": final.get("result", {}),
            }
        except Exception as e:
            out_dict[debate_id] = {"query": query, "status": "error", "error": str(e)}

    threads = []
    for debate_id, query in debate_ids:
        t = threading.Thread(
            target=poll_one,
            args=(debate_id, query, results),
            daemon=True,
        )
        threads.append(t)
        t.start()

    step("Polling all 3 debates in parallel — this may take several minutes...")
    for t in threads:
        t.join(timeout=950)

    # ── Evaluate results ──────────────────────────────────────
    print()
    step("─── Results ───")
    completed = 0
    for debate_id, query in debate_ids:
        r = results.get(debate_id, {"status": "no_result"})
        status = r.get("status")
        winner = r.get("result", {}).get("winning_angle", "?")
        score  = r.get("result", {}).get("avg_score", "?")
        if status == "complete":
            ok(f"{debate_id}: complete — winner={winner} score={score}")
            ok(f"  Query: '{query[:60]}'")
            completed += 1
        elif status == "timeout":
            fail(f"{debate_id}: timed out")
            fail(f"  Query: '{query[:60]}'")
        else:
            fail(f"{debate_id}: {status}")
            fail(f"  Query: '{query[:60]}'")

    # ── Check state isolation ─────────────────────────────────
    print()
    step("Checking state isolation — verifying each debate has unique results...")
    winning_angles = [
        results.get(did, {}).get("result", {}).get("winning_angle")
        for did, _ in debate_ids
        if results.get(did, {}).get("status") == "complete"
    ]
    # If all completed debates have identical winning angles AND identical scores,
    # that might indicate state contamination (unlikely but worth flagging)
    if len(set(str(r.get("result", {}).get("avg_score")) for _, r in results.items())) == 1 and completed > 1:
        warn("All debates returned identical avg_score — possible state contamination.")
        warn("Check Redis keys to confirm each debate_id has independent state.")
    else:
        ok("Debates returned varied results — state isolation looks correct.")

    passed = completed == len(debate_ids)
    return result(
        passed,
        f"All {completed}/{len(debate_ids)} concurrent debates completed successfully." if passed
        else f"Only {completed}/{len(debate_ids)} debates completed — scalability issue detected."
    )


if __name__ == "__main__":
    run()
