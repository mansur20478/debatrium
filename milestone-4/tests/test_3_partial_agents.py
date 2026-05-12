"""
Test 3 — Partial Agent Failure (1 of 3 agents survives)
─────────────────────────────────────────────────────────────
Principle: High Availability — system degrades gracefully

What happens:
  3 research tasks are dispatched per round. If only 1 agent
  survives, it must process all 3 tasks sequentially instead
  of in parallel. The debate should still complete — just slower.

Expected: Debate completes with 1 agent (slower, but no failure).
Bug to find: Aggregator Lambda times out (300s) waiting for all 3
             results before the single agent can process them all,
             leaving the debate in a permanently incomplete state.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, start_debate, wait_for_complete,
    get_agent_instances, get_asg_name,
    ec2, asg,
)


def run() -> bool:
    section("Test 3 — Partial Agent Failure (1 of 3 survives)")

    token = load_token()

    # ── Find the research ASG ─────────────────────────────────
    step("Looking for research-agent ASG...")
    asg_name = get_asg_name("research-agent")
    if not asg_name:
        warn("No research-agent ASG found — checking for direct EC2 instances...")

    instances = get_agent_instances("research-agent")
    step(f"Currently running research agents: {len(instances)} — {instances}")

    if len(instances) < 2:
        warn("Less than 2 research agents running. Test will still run with current agents.")

    original_count = len(instances)

    # ── Terminate all but 1 ───────────────────────────────────
    victims = instances[1:]  # keep instances[0] alive
    if victims:
        step(f"Terminating {len(victims)} agent(s), keeping 1 alive: {instances[0]}")
        ec2.terminate_instances(InstanceIds=victims)
        ok(f"Terminated: {victims}")
        step("Waiting 10s for terminations to register...")
        time.sleep(10)
    else:
        ok("Only 1 agent running — proceeding with single-agent test")

    # ── Start a debate ────────────────────────────────────────
    step("Starting debate with reduced agent capacity...")
    resp      = start_debate("Is remote work more productive than office work?", token)
    debate_id = resp.get("debate_id")
    ok(f"Debate started: {debate_id}")
    step("This debate requires 1 agent to process all 3 research tasks sequentially")

    # ── Poll with extended timeout ────────────────────────────
    step("Polling for result — allowing extended time (timeout=900s)...")
    start    = time.time()
    final    = wait_for_complete(debate_id, token, timeout=900)
    duration = int(time.time() - start)
    status   = final.get("status")

    # ── Report ────────────────────────────────────────────────
    if status == "complete":
        ok(f"Debate completed in {duration}s with reduced agents.")
        ok(f"Winning angle: {final.get('result', {}).get('winning_angle', '?')}")
        if duration > 300:
            warn(f"Took {duration}s — aggregator Lambda may have been close to timeout (300s).")
            warn("Consider increasing aggregator Lambda timeout or reducing VISIBILITY_TIMEOUT.")
        return result(True, f"System degraded gracefully — completed in {duration}s with 1 agent.")
    elif status == "timeout":
        fail(f"Debate did not complete in 900s.")
        fail("Likely cause: aggregator Lambda timed out waiting for results from the single agent.")
        fail("Fix: increase aggregator Lambda timeout, or reduce VISIBILITY_TIMEOUT so tasks retry faster.")
        return result(False, "Graceful degradation failed — debate hung with 1 agent.")
    else:
        fail(f"Unexpected status: {status}")
        return result(False, f"Unexpected final status: {status}")


if __name__ == "__main__":
    run()
