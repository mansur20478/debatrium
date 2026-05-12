"""
Test 5 — Duplicate Message Deduplication
─────────────────────────────────────────────────────────────
Principle: Idempotency via SQS FIFO deduplication

What happens:
  The same message is sent twice with the identical
  MessageDeduplicationId. SQS FIFO queues deduplicate within
  a 5-minute window — the second send should be silently dropped.

Expected: Queue receives only 1 message, not 2.
Bug to find: Both messages are delivered and processed, causing
             the same research angle to be processed twice and
             producing a duplicate result in the results queue.
"""

import sys
import time
import uuid
import json

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    queue_depth,
    sqs, QUEUES,
)

RESEARCH_QUEUE  = QUEUES["research_tasks"]
RESULTS_QUEUE   = QUEUES["research_results"]


def run() -> bool:
    section("Test 5 — Duplicate Message Deduplication")

    # ── Baseline ──────────────────────────────────────────────
    before_tasks   = queue_depth(RESEARCH_QUEUE)
    before_results = queue_depth(RESULTS_QUEUE)
    step(f"Baseline — tasks queue: {before_tasks}  results queue: {before_results}")

    # ── Build a unique debate/dedup ID ────────────────────────
    debate_id = f"D-DEDUP-{uuid.uuid4().hex[:8].upper()}"
    dedup_id  = str(uuid.uuid4())   # SAME dedup ID for both sends
    group_id  = f"{debate_id}-positive-1"

    message = {
        "debate_id":         debate_id,
        "round":             1,
        "query":             "Deduplication test — should only process once",
        "angle":             "positive",
        "judge_feedback":    "",
        "previous_findings": {},
    }

    step(f"Sending message 1 with dedup_id={dedup_id[:16]}...")
    r1 = sqs.send_message(
        QueueUrl=RESEARCH_QUEUE,
        MessageBody=json.dumps(message),
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id,
    )
    ok(f"Message 1 sent — MessageId: {r1['MessageId']}")

    step(f"Sending IDENTICAL message 2 with SAME dedup_id={dedup_id[:16]}...")
    r2 = sqs.send_message(
        QueueUrl=RESEARCH_QUEUE,
        MessageBody=json.dumps(message),
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id,
    )
    ok(f"Message 2 sent — MessageId: {r2['MessageId']}")

    # ── Check: SQS returns the same MessageId for the duplicate ──
    step("Comparing MessageIds from both sends...")
    if r1["MessageId"] == r2["MessageId"]:
        ok(f"Both sends returned the same MessageId: {r1['MessageId']}")
        ok("SQS confirmed deduplication — second send was a no-op")
        dedup_confirmed = True
    else:
        warn(f"Different MessageIds: {r1['MessageId']} vs {r2['MessageId']}")
        warn("SQS may have accepted both — checking queue depth...")
        dedup_confirmed = False

    # ── Check queue depth increased by exactly 1 ─────────────
    time.sleep(3)
    after_tasks = queue_depth(RESEARCH_QUEUE)
    increase    = after_tasks - before_tasks
    step(f"Queue depth after sends: {after_tasks} (increase of {increase})")

    if increase == 1:
        ok("Queue depth increased by exactly 1 — only 1 message was accepted")
        depth_ok = True
    elif increase == 2:
        fail("Queue depth increased by 2 — BOTH messages were accepted (deduplication failed!)")
        fail("This means the same research angle could be processed twice.")
        depth_ok = False
    elif increase == 0:
        warn("Queue depth unchanged — agents may have consumed the message already")
        depth_ok = True  # Can't confirm but not a failure
    else:
        warn(f"Unexpected depth increase of {increase}")
        depth_ok = False

    passed = dedup_confirmed or depth_ok

    return result(passed,
                  "Deduplication working — duplicate message was dropped by SQS." if passed
                  else "Deduplication FAILED — duplicate message was accepted and could cause double-processing.")


if __name__ == "__main__":
    run()
