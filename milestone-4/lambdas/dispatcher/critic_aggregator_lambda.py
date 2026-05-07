"""
critic_aggregator_lambda.py — Critic Results Aggregator + Judge Dispatcher


Input message format (from critic agents):
{
    "debate_id":   "D-001",
    "round":       1,
    "critic_slot": 2,
    "lens":        "evidence",
    "critique":    { "overall_score": 0.7, ... },
    "agent_id":    "CA-i-0abc123",
    "timestamp":   1234567890
}
"""

import os
import json
import time
import hashlib
import logging
import boto3
import redis

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log(msg):  print(f"→ {msg}")
def ok(msg):   print(f"✓ {msg}")
def warn(msg): print(f"⚠ {msg}")
def fail(msg): print(f"✗ {msg}")


# ENVIRONMENT

REGION            = os.environ.get("AWS_REGION", "us-east-1")
REDIS_HOST        = os.environ["REDIS_HOST"]
REDIS_PORT        = int(os.environ.get("REDIS_PORT", 6379))
REDIS_AUTH_TOKEN  = os.environ.get("REDIS_AUTH_TOKEN", "")
JUDGE_QUEUE_URL   = os.environ["JUDGE_TASKS_QUEUE_URL"]
NUM_JUDGE_SLOTS   = int(os.environ.get("NUM_JUDGE_SLOTS", 3))
EXPECTED_CRITIQUES = 3   # logical + evidence + completeness

KEY_TTL = 7200


# CLIENTS

sqs = boto3.client("sqs", region_name=REGION)


def get_redis() -> redis.Redis:
    kwargs = {
        "host": REDIS_HOST, "port": REDIS_PORT,
        "decode_responses": True, "ssl": True,
        "socket_timeout": 5, "socket_connect_timeout": 5,
    }
    if REDIS_AUTH_TOKEN:
        kwargs["password"] = REDIS_AUTH_TOKEN
    r = redis.Redis(**kwargs)
    r.ping()
    ok("Redis connection established")
    return r



# REDIS OPERATIONS

def store_critique(r, debate_id, round_num, lens, critique) -> int:
    crit_key  = f"crit:{debate_id}:r{round_num}"
    count_key = f"crit_count:{debate_id}:r{round_num}"

    log(f"Writing critique to Redis — key={crit_key} lens={lens}")
    pipe = r.pipeline()
    pipe.hset(crit_key,  lens, json.dumps(critique))
    pipe.expire(crit_key, KEY_TTL)
    pipe.incr(count_key)
    pipe.expire(count_key, KEY_TTL)
    results   = pipe.execute()
    new_count = results[2]

    ok(f"Critique stored — lens={lens} | progress={new_count}/{EXPECTED_CRITIQUES}")
    return new_count


def get_all_critiques(r, debate_id, round_num) -> dict:
    crit_key = f"crit:{debate_id}:r{round_num}"
    raw = r.hgetall(crit_key)
    return {lens: json.loads(critique) for lens, critique in raw.items()}


def get_all_research_results(r, debate_id, round_num) -> dict:
    """Read research findings so judges see both research + critiques."""
    res_key = f"res:{debate_id}:r{round_num}"
    raw = r.hgetall(res_key)
    return {angle: json.loads(findings) for angle, findings in raw.items()}


def get_query(r, debate_id) -> str:
    return r.get(f"query:{debate_id}") or ""


def mark_dispatched(r, debate_id, round_num, stage) -> bool:
    key    = f"dispatched:{stage}:{debate_id}:r{round_num}"
    result = r.set(key, "true", nx=True, ex=KEY_TTL)
    return result is True


def get_unique_lenses(r, debate_id, round_num) -> int:
    crit_key = f"crit:{debate_id}:r{round_num}"
    return r.hlen(crit_key)



# JUDGE DISPATCH

def dispatch_to_judges(debate_id, round_num, query, all_critiques, all_research):
    """
    Fan out to NUM_JUDGE_SLOTS judge agents.
    Each judge slot gets:
      - the full research bundle (all angles)
      - the full critique bundle (all lenses)
      - its own judge_slot number (maps to a judging lens)
    """
    log(f"Dispatching to {NUM_JUDGE_SLOTS} judge slots — debate={debate_id} round={round_num}")

    bundle = {
        "debate_id":   debate_id,
        "round":       round_num,
        "query":       query,
        "research":    all_research,   # { angle:   findings  }
        "critiques":   all_critiques,  # { lens:    critique  }
        "timestamp":   time.time(),
    }

    for slot in range(1, NUM_JUDGE_SLOTS + 1):
        group_id = f"{debate_id}-judge-slot{slot}-r{round_num}"
        dedup_id = hashlib.md5(
            f"{debate_id}-judge-r{round_num}-slot{slot}".encode()
        ).hexdigest()

        sqs.send_message(
            QueueUrl=JUDGE_QUEUE_URL,
            MessageBody=json.dumps({**bundle, "judge_slot": slot}),
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )
        ok(f"Judge slot {slot} dispatched — group={group_id}")

    ok(f"All {NUM_JUDGE_SLOTS} judge slots dispatched for debate={debate_id} round={round_num}")



# MAIN HANDLER

def lambda_handler(event, context):
    record_count = len(event.get("Records", []))
    print("\n" + "="*60)
    print(f"  CRITIC AGGREGATOR LAMBDA")
    print(f"  Records in batch : {record_count}")
    print(f"  Log stream       : {context.log_stream_name}")
    print("="*60)

    try:
        r = get_redis()
    except Exception as e:
        fail(f"Redis connection failed — aborting all {record_count} records: {e}")
        return {
            "batchItemFailures": [
                {"itemIdentifier": rec["messageId"]}
                for rec in event.get("Records", [])
            ]
        }

    failed_message_ids = []

    for i, record in enumerate(event.get("Records", []), 1):
        message_id = record["messageId"]
        print(f"\n─── Record {i}/{record_count} — messageId={message_id} ───")

        try:
            # ── 1. Parse ──────────────────────────────────────────
            log("Stage 1 — Parsing SQS message")
            payload    = json.loads(record["body"])
            debate_id  = payload["debate_id"]
            round_num  = int(payload["round"])
            lens       = payload["lens"]
            critique   = payload["critique"]
            ok(f"Parsed — debate={debate_id} round={round_num} lens={lens}")

            # ── 2. Store critique in Redis ────────────────────────
            log("Stage 2 — Storing critique in Redis")
            store_critique(r, debate_id, round_num, lens, critique)

            # ── 3. Check unique lenses (not raw count) ────────────
            log("Stage 3 — Checking unique lenses received")
            unique_lenses = get_unique_lenses(r, debate_id, round_num)
            print(f"  Unique lenses: {unique_lenses}/{EXPECTED_CRITIQUES}")

            if unique_lenses < EXPECTED_CRITIQUES:
                warn(f"Waiting for {EXPECTED_CRITIQUES - unique_lenses} more lens(es)")
                continue

            ok(f"All {EXPECTED_CRITIQUES} critique lenses received")

            # ── 4. Dispatch lock ──────────────────────────────────
            log("Stage 4 — Acquiring dispatch lock")
            if not mark_dispatched(r, debate_id, round_num, "critic"):
                warn("Another invocation already dispatched to judges — skipping")
                continue
            ok("Dispatch lock acquired")

            # ── 5. Load full bundles ──────────────────────────────
            log("Stage 5 — Loading full bundles from Redis")
            all_critiques = get_all_critiques(r, debate_id, round_num)
            all_research  = get_all_research_results(r, debate_id, round_num)
            query         = get_query(r, debate_id)
            ok(f"Loaded {len(all_critiques)} critiques and {len(all_research)} research results")

            # ── 6. Dispatch to judges ─────────────────────────────
            log("Stage 6 — Dispatching to judge agents")
            dispatch_to_judges(debate_id, round_num, query, all_critiques, all_research)
            ok(f"Record {i} fully processed ✓")

        except KeyError as e:
            fail(f"Malformed payload — missing field: {e}")
            logger.error(f"Missing field {e} in record {message_id}", exc_info=True)
            failed_message_ids.append(message_id)

        except redis.RedisError as e:
            fail(f"Redis error on record {message_id}: {e}")
            logger.error(f"Redis error: {e}", exc_info=True)
            failed_message_ids.append(message_id)

        except Exception as e:
            fail(f"Unexpected error on record {message_id}: {e}")
            logger.error(f"Unexpected error: {e}", exc_info=True)
            failed_message_ids.append(message_id)

    print("\n" + "="*60)
    succeeded = record_count - len(failed_message_ids)
    print(f"  BATCH COMPLETE — {succeeded}/{record_count} succeeded")
    if failed_message_ids:
        warn(f"{len(failed_message_ids)} record(s) failed")
        print("="*60 + "\n")
        return {
            "batchItemFailures": [
                {"itemIdentifier": mid} for mid in failed_message_ids
            ]
        }

    print("="*60 + "\n")
    return {"statusCode": 200, "body": "OK"}