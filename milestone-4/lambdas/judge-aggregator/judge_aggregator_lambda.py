"""
judge_aggregator_lambda.py — Judge Results Aggregator + Round Controller
Triggered by SQS Event Source Mapping on `judge-results` queue.

Flow:
  1. Parse incoming judge verdict from SQS record body
  2. Write verdict to Redis hash  (key: judge:<debate_id>:r<round>)
  3. Atomically increment verdict counter
  4. If all 3 judge lenses in → compute average score → decide:
       score >= 0.85 OR round == MAX_ROUNDS → FINALIZE
       otherwise                            → NEXT ROUND
  5. FINALIZE: write final result to Redis + S3, mark debate complete
  6. NEXT ROUND: read judge feedback, dispatch new research tasks with feedback
  7. SET NX dispatch lock to prevent duplicate round transitions
"""

import os
import json
import uuid
import time
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

REGION               = os.environ.get("AWS_REGION", "us-east-1")
REDIS_HOST           = os.environ["REDIS_HOST"]
REDIS_PORT           = int(os.environ.get("REDIS_PORT", 6379))
REDIS_AUTH_TOKEN     = os.environ.get("REDIS_AUTH_TOKEN", "")
RESEARCH_QUEUE_URL   = os.environ["RESEARCH_TASKS_QUEUE_URL"]
FINAL_RESULTS_QUEUE  = os.environ["FINAL_RESULTS_QUEUE_URL"]
S3_BUCKET            = os.environ["S3_BUCKET"]
MAX_ROUNDS           = int(os.environ.get("MAX_ROUNDS", 3))
SCORE_THRESHOLD      = float(os.environ.get("SCORE_THRESHOLD", 0.85))
EXPECTED_VERDICTS    = 3   # fairness + accuracy + consensus

RESEARCH_ANGLES = ["positive", "neutral", "negative"]
KEY_TTL         = 86400   # 24 hours


# CLIENTS

sqs = boto3.client("sqs", region_name=REGION)
s3  = boto3.client("s3",  region_name=REGION)


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

def store_verdict(r, debate_id, round_num, lens, verdict) -> int:
    judge_key = f"judge:{debate_id}:r{round_num}"
    count_key = f"judge_count:{debate_id}:r{round_num}"

    pipe = r.pipeline()
    pipe.hset(judge_key,  lens, json.dumps(verdict))
    pipe.expire(judge_key,  KEY_TTL)
    pipe.incr(count_key)
    pipe.expire(count_key,  KEY_TTL)
    results   = pipe.execute()
    new_count = results[2]
    ok(f"Verdict stored — lens={lens} | progress={new_count}/{EXPECTED_VERDICTS}")
    return new_count


def get_all_verdicts(r, debate_id, round_num) -> dict:
    judge_key = f"judge:{debate_id}:r{round_num}"
    raw = r.hgetall(judge_key)
    return {lens: json.loads(v) for lens, v in raw.items()}


def get_unique_verdicts(r, debate_id, round_num) -> int:
    return r.hlen(f"judge:{debate_id}:r{round_num}")


def get_all_research(r, debate_id, round_num) -> dict:
    raw = r.hgetall(f"res:{debate_id}:r{round_num}")
    return {angle: json.loads(f) for angle, f in raw.items()}


def get_query(r, debate_id) -> str:
    return r.get(f"query:{debate_id}") or ""


def get_max_rounds(r, debate_id) -> int:
    stored = r.get(f"max_rounds:{debate_id}")
    return int(stored) if stored else MAX_ROUNDS


def get_threshold(r, debate_id) -> float:
    stored = r.get(f"threshold:{debate_id}")
    return float(stored) if stored else SCORE_THRESHOLD


def mark_dispatched(r, debate_id, round_num, stage) -> bool:
    key    = f"dispatched:{stage}:{debate_id}:r{round_num}"
    result = r.set(key, "true", nx=True, ex=KEY_TTL)
    return result is True



# SCORE CALCULATION

def compute_average_score(all_verdicts: dict) -> float:
    """
    Average the overall_score from all judge lenses.
    Each judge verdict must contain an overall_score float 0.0-1.0.
    """
    scores = []
    for lens, verdict in all_verdicts.items():
        score = verdict.get("overall_score")
        if score is not None:
            scores.append(float(score))
            log(f"  Judge [{lens}] score: {score}")
        else:
            warn(f"  Judge [{lens}] missing overall_score — skipping")

    if not scores:
        warn("No valid scores found — defaulting to 0.0")
        return 0.0

    avg = sum(scores) / len(scores)
    ok(f"Average judge score: {avg:.3f}")
    return avg


def merge_feedback(all_verdicts: dict) -> dict:
    """
    Merge per-angle feedback from all judge lenses into one dict.
    { angle: [feedback from fairness, feedback from accuracy, ...] }
    Then join into a single string per angle for the research agents.
    """
    merged = {angle: [] for angle in RESEARCH_ANGLES}

    for lens, verdict in all_verdicts.items():
        feedback = verdict.get("feedback_for_next_round", {})
        for angle in RESEARCH_ANGLES:
            note = feedback.get(angle, "")
            if note:
                merged[angle].append(f"[{lens}]: {note}")

    # Join multiple feedback notes into one string per angle
    return {
        angle: " | ".join(notes) if notes else "No specific feedback — maintain quality."
        for angle, notes in merged.items()
    }



# FINALIZE DEBATE

def finalize_debate(r, debate_id, round_num, query, all_verdicts, avg_score, stop_reason):
    """
    Write final result to Redis and S3.
    Mark debate as complete.
    Notify final-results queue.
    """
    log(f"Finalizing debate — debate={debate_id} round={round_num} reason={stop_reason}")

    # Build final result
    all_research = get_all_research(r, debate_id, round_num)
    final_result = {
        "debate_id":    debate_id,
        "query":        query,
        "total_rounds": round_num,
        "stop_reason":  stop_reason,
        "avg_score":    avg_score,
        "verdicts":     all_verdicts,
        "final_research": all_research,
        "completed_at": time.time(),
    }

    # Determine winning angle from verdicts
    angle_scores = {angle: [] for angle in RESEARCH_ANGLES}
    for lens, verdict in all_verdicts.items():
        per_angle = verdict.get("per_angle_scores", {})
        for angle, score in per_angle.items():
            if angle in angle_scores:
                angle_scores[angle].append(float(score))

    angle_averages = {
        angle: sum(scores) / len(scores) if scores else 0.0
        for angle, scores in angle_scores.items()
    }
    winning_angle = max(angle_averages, key=angle_averages.get)
    final_result["angle_scores"]  = angle_averages
    final_result["winning_angle"] = winning_angle

    ok(f"Winning angle: {winning_angle} (score: {angle_averages[winning_angle]:.3f})")

    # ── Write to Redis ────────────────────────────────────────
    r.set(f"final_result:{debate_id}", json.dumps(final_result), ex=KEY_TTL)
    r.set(f"status:{debate_id}",       "complete",               ex=KEY_TTL)
    ok("Final result written to Redis")

    # ── Write to S3 ───────────────────────────────────────────
    s3_key = f"debates/{debate_id}/final_result.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(final_result, indent=2),
        ContentType="application/json",
    )
    ok(f"Final result written to S3 — s3://{S3_BUCKET}/{s3_key}")

    # ── Notify final-results queue ────────────────────────────
    sqs.send_message(
        QueueUrl=FINAL_RESULTS_QUEUE,
        MessageBody=json.dumps({
            "debate_id":     debate_id,
            "status":        "complete",
            "avg_score":     avg_score,
            "winning_angle": winning_angle,
            "stop_reason":   stop_reason,
            "s3_key":        s3_key,
        }),
        MessageGroupId=debate_id,
        MessageDeduplicationId=f"{debate_id}-final",
    )
    ok(f"Final result notification sent to final-results queue")



# DISPATCH NEXT ROUND

def dispatch_next_round(r, debate_id, round_num, query, all_verdicts):
    """
    Start round N+1 by sending new research tasks with judge feedback.
    Each research task includes:
      - previous_findings: what the agent found last round
      - judge_feedback:    what judges said needs improving
    """
    next_round = round_num + 1
    log(f"Starting round {next_round} — debate={debate_id}")

    # Update current round in Redis
    r.set(f"current_round:{debate_id}", str(next_round), ex=KEY_TTL)

    # Merge all judge feedback into one string per angle
    judge_feedback = merge_feedback(all_verdicts)
    log(f"Merged judge feedback for angles: {list(judge_feedback.keys())}")

    # Load previous findings so agents can build on them
    previous_findings = get_all_research(r, debate_id, round_num)

    for angle in RESEARCH_ANGLES:
        task = {
            "debate_id":         debate_id,
            "round":             next_round,
            "query":             query,
            "angle":             angle,
            "judge_feedback":    judge_feedback.get(angle, ""),
            "previous_findings": previous_findings.get(angle, {}),
        }

        group_id = f"{debate_id}-{angle}-{next_round}"
        dedup_id = str(uuid.uuid4())

        sqs.send_message(
            QueueUrl=RESEARCH_QUEUE_URL,
            MessageBody=json.dumps(task),
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )
        ok(f"Round {next_round} research task sent — angle={angle}")

    ok(f"All round {next_round} tasks dispatched — debate={debate_id}")



# MAIN HANDLER

def lambda_handler(event, context):
    record_count = len(event.get("Records", []))
    print("\n" + "="*60)
    print(f"  JUDGE AGGREGATOR LAMBDA")
    print(f"  Records in batch : {record_count}")
    print(f"  Log stream       : {context.log_stream_name}")
    print("="*60)

    try:
        r = get_redis()
    except Exception as e:
        fail(f"Redis connection failed: {e}")
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
            log("Stage 1 — Parsing judge verdict")
            payload   = json.loads(record["body"])
            debate_id = payload["debate_id"]
            round_num = int(payload["round"])
            lens      = payload["lens"]
            verdict   = payload["verdict"]
            ok(f"Parsed — debate={debate_id} round={round_num} lens={lens}")

            # ── 2. Store verdict ──────────────────────────────────
            log("Stage 2 — Storing verdict in Redis")
            store_verdict(r, debate_id, round_num, lens, verdict)

            # ── 3. Check unique verdicts ──────────────────────────
            log("Stage 3 — Checking unique verdicts")
            unique = get_unique_verdicts(r, debate_id, round_num)
            print(f"  Unique verdicts: {unique}/{EXPECTED_VERDICTS}")

            if unique < EXPECTED_VERDICTS:
                warn(f"Waiting for {EXPECTED_VERDICTS - unique} more verdict(s)")
                continue

            ok(f"All {EXPECTED_VERDICTS} judge verdicts received")

            # ── 4. Dispatch lock ──────────────────────────────────
            log("Stage 4 — Acquiring dispatch lock")
            if not mark_dispatched(r, debate_id, round_num, "judge"):
                warn("Another invocation already handled this round — skipping")
                continue
            ok("Dispatch lock acquired")

            # ── 5. Load all verdicts + compute score ──────────────
            log("Stage 5 — Computing average score")
            all_verdicts = get_all_verdicts(r, debate_id, round_num)
            query        = get_query(r, debate_id)
            avg_score    = compute_average_score(all_verdicts)
            max_rounds   = get_max_rounds(r, debate_id)
            threshold    = get_threshold(r, debate_id)

            print(f"  Average score : {avg_score:.3f}")
            print(f"  Threshold     : {threshold}")
            print(f"  Round         : {round_num}/{max_rounds}")

            # ── 6. Decide: finalize or next round ─────────────────
            log("Stage 6 — Round decision")

            if avg_score >= threshold:
                stop_reason = f"score_threshold_met (score={avg_score:.3f} >= {threshold})"
                ok(f"Score threshold met — finalizing debate")
                finalize_debate(r, debate_id, round_num, query, all_verdicts, avg_score, stop_reason)

            elif round_num >= max_rounds:
                stop_reason = f"max_rounds_reached (round={round_num}/{max_rounds})"
                ok(f"Max rounds reached — finalizing debate")
                finalize_debate(r, debate_id, round_num, query, all_verdicts, avg_score, stop_reason)

            else:
                ok(f"Score {avg_score:.3f} below threshold {threshold} — starting round {round_num + 1}")
                dispatch_next_round(r, debate_id, round_num, query, all_verdicts)

            ok(f"Record {i} fully processed ✓")

        except KeyError as e:
            fail(f"Malformed payload — missing field: {e}")
            logger.error(f"Missing field {e} in record {message_id}", exc_info=True)
            failed_message_ids.append(message_id)

        except redis.RedisError as e:
            fail(f"Redis error: {e}")
            logger.error(f"Redis error: {e}", exc_info=True)
            failed_message_ids.append(message_id)

        except Exception as e:
            fail(f"Unexpected error: {e}")
            logger.error(f"Unexpected error: {e}", exc_info=True)
            failed_message_ids.append(message_id)

    print("\n" + "="*60)
    succeeded = record_count - len(failed_message_ids)
    print(f"  BATCH COMPLETE — {succeeded}/{record_count} succeeded")
    if failed_message_ids:
        print("="*60 + "\n")
        return {
            "batchItemFailures": [
                {"itemIdentifier": mid} for mid in failed_message_ids
            ]
        }

    print("="*60 + "\n")
    return {"statusCode": 200, "body": "OK"}