"""
Test 7 — S3 Write Failure (Cascading Failure)
─────────────────────────────────────────────────────────────
Principle: Resilience — what happens when a downstream store fails

What happens:
  The S3 bucket policy is removed, making the bucket reject writes
  from the judge-aggregator Lambda. The final result is still written
  to Redis (status:complete), but the S3 backup copy is lost.

Expected:
  - Debate API still returns the result (from Redis) ✓
  - S3 has no copy of the result (data durability risk)
  - Judge-aggregator Lambda logs an S3 error

Bug to find:
  - Judge-aggregator sets status:complete in Redis AFTER S3 write fails,
    causing the debate to appear "complete" with no result in Redis either
  - OR: Judge-aggregator exception prevents status from being set at all,
    leaving the debate in permanent "running" state

ALWAYS restores the S3 bucket policy in the finally block.
"""

import sys
import time
import json

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, start_debate, wait_for_complete,
    remove_s3_policy, restore_s3_policy,
    recent_lambda_errors,
    s3, S3_BUCKET, ACCOUNT_ID, REGION,
)


def s3_object_exists(debate_id: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"results/{debate_id}.json")
        return True
    except Exception:
        return False


def run() -> bool:
    section("Test 7 — S3 Write Failure (Cascading Failure)")

    token  = load_token()
    passed = False

    step("Removing S3 bucket policy — writes from Lambda will be rejected...")
    try:
        remove_s3_policy()
        ok(f"Bucket policy removed from {S3_BUCKET}")
    except Exception as e:
        warn(f"Could not remove bucket policy: {e}")
        warn("Bucket may already have no policy — continuing test")

    try:
        # ── Start a debate and let it run to completion ───────
        step("Starting debate — will run all 3 rounds with S3 unavailable...")
        resp      = start_debate("Is gene editing ethically acceptable?", token)
        debate_id = resp.get("debate_id")
        ok(f"Debate started: {debate_id}")

        step("Polling for result — this may take several minutes...")
        final  = wait_for_complete(debate_id, token, timeout=900)
        status = final.get("status")

        # ── Check 1: API still returns result (Redis is source of truth) ──
        print()
        step("─── Check 1: API result availability ───")
        if status == "complete":
            ok("Debate completed — result available via API (from Redis)")
            ok(f"Winning angle: {final.get('result', {}).get('winning_angle', '?')}")
            api_ok = True
        elif status == "timeout":
            fail("Debate timed out — S3 failure may have prevented result from being set in Redis")
            fail("This is the worst-case bug: result is completely lost")
            api_ok = False
        else:
            fail(f"Unexpected status: {status}")
            api_ok = False

        # ── Check 2: S3 has no copy (expected — policy was removed) ──
        print()
        step("─── Check 2: S3 backup status ───")
        in_s3 = s3_object_exists(debate_id)
        if in_s3:
            warn("Result WAS written to S3 despite policy removal — permissions may not have taken effect")
        else:
            ok("Result NOT in S3 — confirms bucket policy removal worked")
            ok("This means if Redis TTL expires (24h), result will be permanently lost")
            warn("Improvement: judge-aggregator should retry S3 write or use a DLQ for failed writes")

        # ── Check 3: Lambda error logs ────────────────────────
        print()
        step("─── Check 3: Lambda error logs ───")
        errors = recent_lambda_errors("judge_aggregator_lambda", seconds=900)
        s3_errors = [e for e in errors if "S3" in e or "AccessDenied" in e or "NoSuchBucket" in e]
        if s3_errors:
            ok(f"Found {len(s3_errors)} S3 error(s) in judge-aggregator logs:")
            for e in s3_errors[:3]:
                step(f"  {e[:120]}")
            ok("Error was logged correctly — good observability")
        else:
            warn("No S3 errors found in Lambda logs — either S3 write succeeded or error was silently swallowed")

        passed = api_ok

    finally:
        # ── Always restore S3 policy ──────────────────────────
        print()
        step("Restoring S3 bucket policy...")
        restore_s3_policy()
        ok(f"S3 bucket policy restored for {S3_BUCKET}")

    return result(
        passed,
        "S3 failure handled gracefully — result available from Redis despite S3 being down." if passed
        else "S3 failure caused result loss — judge-aggregator does not decouple Redis from S3 write."
    )


if __name__ == "__main__":
    run()
