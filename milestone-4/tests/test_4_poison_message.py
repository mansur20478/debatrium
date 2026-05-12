"""
Test 4 — Poison Message (Malformed Task)
─────────────────────────────────────────────────────────────
Principle: Error isolation — bad messages must not block the queue

What happens:
  A malformed JSON message (missing required fields) is injected
  directly into the research-tasks queue. The agent's KeyError
  handler should catch it, log it, and delete it — unblocking
  the queue for legitimate tasks.

Expected:
  - Queue depth returns to normal after the poison message
  - A real debate started after the injection completes normally
  - DLQ receives the message if delete_message fails

Bug to find:
  - Agent crashes entirely (exits) on the bad message
  - Message is NOT deleted and re-delivered indefinitely
  - Good messages behind it are blocked (FIFO group ordering)
"""

import sys
import time
import uuid
import json

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, start_debate, wait_for_complete,
    queue_depth, dlq_depth,
    sqs, QUEUES,
)

RESEARCH_QUEUE = QUEUES["research_tasks"]


def run() -> bool:
    section("Test 4 — Poison Message (Malformed Task)")

    token = load_token()

    # ── Baseline queue depth ──────────────────────────────────
    before = queue_depth(RESEARCH_QUEUE)
    step(f"Queue depth before injection: {before}")

    # ── Inject a poison message ───────────────────────────────
    poison = {
        "debate_id": "D-POISON-TEST",
        "broken_field": True,
        # Missing: round, query, angle, judge_feedback, previous_findings
    }
    step(f"Injecting malformed message: {poison}")

    poison_group = f"poison-{uuid.uuid4().hex[:8]}"
    poison_dedup = str(uuid.uuid4())

    sqs.send_message(
        QueueUrl=RESEARCH_QUEUE,
        MessageBody=json.dumps(poison),
        MessageGroupId=poison_group,
        MessageDeduplicationId=poison_dedup,
    )
    ok("Poison message injected into research-tasks queue")

    # ── Check depth increased ─────────────────────────────────
    time.sleep(3)
    after_inject = queue_depth(RESEARCH_QUEUE)
    step(f"Queue depth after injection: {after_inject}")

    # ── Start a real debate in a DIFFERENT group ──────────────
    step("Starting a real debate to verify good messages still process...")
    resp      = start_debate("Is space exploration worth the cost?", token)
    debate_id = resp.get("debate_id")
    ok(f"Real debate started: {debate_id}")

    # ── Wait for agent to process the poison message ──────────
    step("Waiting up to 90s for agent to process poison message...")
    deadline = time.time() + 90
    while time.time() < deadline:
        depth = queue_depth(RESEARCH_QUEUE)
        step(f"  Queue depth: {depth} — waiting for poison to be consumed...")
        if depth <= before:
            ok("Queue depth returned to baseline — poison message was handled")
            break
        time.sleep(15)
    else:
        warn("Queue depth did not return to baseline in 90s")
        warn("Poison message may be re-delivered repeatedly (not deleted on error)")

    # ── Check DLQ ─────────────────────────────────────────────
    dlq = dlq_depth(RESEARCH_QUEUE)
    if dlq == -1:
        warn("DLQ not found — skipping DLQ check")
    elif dlq > 0:
        ok(f"DLQ has {dlq} message(s) — poison message was quarantined correctly")
    else:
        step("DLQ is empty — agent deleted the message directly (also acceptable)")

    # ── Verify real debate still completes ────────────────────
    step(f"Polling real debate {debate_id} to verify queue is unblocked...")
    final  = wait_for_complete(debate_id, token, timeout=600)
    status = final.get("status")

    if status == "complete":
        ok("Real debate completed — queue is unblocked and processing normally")
        passed = True
    elif status == "timeout":
        fail("Real debate timed out — poison message may have disrupted queue processing")
        passed = False
    else:
        fail(f"Real debate ended with unexpected status: {status}")
        passed = False

    return result(passed, "Poison message handled correctly — queue unblocked." if passed
                  else "Poison message disrupted queue — error isolation failed.")


if __name__ == "__main__":
    run()
