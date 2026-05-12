"""
Test 1 — Agent Crash Mid-Processing
─────────────────────────────────────────────────────────────
Principle: Fault Tolerance via Heartbeat + Visibility Timeout

What happens:
  A research agent EC2 instance is terminated while a debate is
  running. The heartbeat thread dies with it. After VISIBILITY_TIMEOUT
  (30s) the SQS message reappears and a surviving agent picks it up.

Expected: Debate completes despite the crash.
Bug to find: Debate hangs permanently if no surviving agents exist
             or if ASG does not replace the instance fast enough.
"""

import sys
import time
sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, start_debate, wait_for_complete,
    get_agent_instances, ec2,
)


def run() -> bool:
    section("Test 1 — Agent Crash Mid-Processing")

    token = load_token()

    # ── Check at least 2 research agents are running ──────────
    step("Finding running research-agent instances...")
    instances = get_agent_instances("research-agent")
    if len(instances) < 2:
        warn(f"Only {len(instances)} research agent(s) running — need ≥ 2 for this test.")
        warn("Start more instances via the ASG or launch template, then re-run.")
        return result(False, "Skipped — not enough agents to crash one safely.")

    ok(f"Found {len(instances)} running research agents: {instances}")

    # ── Start a debate ────────────────────────────────────────
    step("Starting debate...")
    resp      = start_debate("Should autonomous vehicles be allowed on public roads?", token)
    debate_id = resp.get("debate_id")
    ok(f"Debate started: {debate_id}")

    # ── Wait 15s then kill one agent ──────────────────────────
    step("Waiting 15s for agents to pick up tasks before crashing one...")
    time.sleep(15)

    victim = instances[0]
    step(f"Terminating instance: {victim}")
    ec2.terminate_instances(InstanceIds=[victim])
    ok(f"Instance {victim} terminated — heartbeat will stop, message will reappear in ~30s")

    # ── Poll for completion ───────────────────────────────────
    step("Polling for debate result (timeout=600s)...")
    final = wait_for_complete(debate_id, token, timeout=600)
    status = final.get("status")

    # ── Verify ────────────────────────────────────────────────
    if status == "complete":
        ok(f"Debate completed successfully despite agent crash.")
        ok(f"Winning angle: {final.get('result', {}).get('winning_angle', '?')}")
        return result(True, "Fault tolerance confirmed — debate survived agent crash.")
    elif status == "timeout":
        fail("Debate did not complete within 10 minutes after crash.")
        fail("Possible causes: all agents dead, ASG too slow to replace, visibility timeout too long.")
        return result(False, "Debate hung after agent crash — fault tolerance failed.")
    else:
        fail(f"Unexpected status: {status}")
        return result(False, f"Unexpected status after crash: {status}")


if __name__ == "__main__":
    run()
