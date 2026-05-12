"""
Test 2 — Redis Network Partition
─────────────────────────────────────────────────────────────
Principle: Dependency failure isolation

What happens:
  The inbound rule allowing Lambda SG → ElastiCache SG on port
  6379 is removed. The orchestrator Lambda cannot reach Redis
  and should return a clean 503 instead of crashing silently.

Expected: API returns 503 "Service unavailable — Redis unreachable"
Bug to find: Unhandled exception leaking internal details, or
             Lambda returning 200 with empty body instead of 503.

ALWAYS restores the security group rule in the finally block.
"""

import sys
import time
import urllib.request
import urllib.error
import json

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from tests.helpers import (
    section, step, ok, warn, fail, result,
    load_token, API_URL,
    block_redis, restore_redis,
)


def run() -> bool:
    section("Test 2 — Redis Network Partition")

    token = load_token()
    passed = False

    step("Blocking Lambda → Redis on port 6379 (removing SG inbound rule)...")
    try:
        block_redis()
        ok("Security group rule removed — Redis is now unreachable from Lambdas")

        # Wait a moment for the rule change to propagate
        step("Waiting 5s for SG change to propagate...")
        time.sleep(5)

        # ── Attempt to start a debate ─────────────────────────
        step("Attempting to start a debate (should fail with 503)...")
        data = json.dumps({"query": "Is Redis highly available?"}).encode()
        req  = urllib.request.Request(
            f"{API_URL}/debate",
            data=data,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body   = json.loads(resp.read().decode())
                status = resp.status
                fail(f"Expected 503 but got HTTP {status}: {body}")
                passed = False

        except urllib.error.HTTPError as e:
            body = json.loads(e.read().decode())
            code = e.code
            msg  = body.get("error", "")

            step(f"Got HTTP {code}: {msg}")

            if code == 503 and "Redis" in msg:
                ok("Correct — got 503 with Redis error message")
                ok("Lambda correctly isolates the Redis failure from the caller")
                passed = True
            elif code == 503:
                warn(f"Got 503 but message was: '{msg}' (expected 'Redis unreachable')")
                passed = True  # Still a 503, acceptable
            else:
                fail(f"Expected 503, got {code}: {msg}")
                passed = False

    finally:
        # ── Always restore ────────────────────────────────────
        step("Restoring security group rule (port 6379)...")
        restore_redis()
        ok("Security group rule restored — Redis is reachable again")

        # Verify restoration
        step("Waiting 5s then verifying API is healthy again...")
        time.sleep(5)
        try:
            data = json.dumps({"query": "health check after restore"}).encode()
            req  = urllib.request.Request(
                f"{API_URL}/debate",
                data=data,
                headers={
                    "Content-Type":  "application/json",
                    "Authorization": f"Bearer {token}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read().decode())
                if body.get("debate_id"):
                    ok(f"API healthy after restore — debate {body['debate_id']} started")
                else:
                    warn(f"Unexpected response after restore: {body}")
        except Exception as e:
            warn(f"API still unhealthy after restore: {e}")

    return result(passed, "Redis partition correctly returned 503 and recovered cleanly." if passed
                  else "Redis partition did not produce expected 503 response.")


if __name__ == "__main__":
    run()
