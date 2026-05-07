"""
orchestrator_lambda.py — Debate Orchestrator
Triggered by API Gateway POST /debate

Responsibilities:
  1. Validate incoming request
  2. Initialize debate state in Redis
  3. Dispatch round 1 research tasks to research-tasks queue
  4. Return debate_id to frontend immediately (async — frontend polls for result)

The debate then runs autonomously:
  Research → 
  Aggregator → 
  Critics → 
  Critic Aggregator → 
  Judges → 
  Judge Aggregator
      ↓
if score < 0.85 and round < 3:
 loop back to research (round+1)
else:
 write final result to Redis + S3

Frontend polls GET /debate/{debate_id}/result to check if done.
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
def fail(msg): print(f"✗ {msg}")


# ENVIRONMENT

REGION              = os.environ.get("AWS_REGION", "us-east-1")
REDIS_HOST          = os.environ["REDIS_HOST"]
REDIS_PORT          = int(os.environ.get("REDIS_PORT", 6379))
REDIS_AUTH_TOKEN    = os.environ.get("REDIS_AUTH_TOKEN", "")
RESEARCH_QUEUE_URL  = os.environ["RESEARCH_TASKS_QUEUE_URL"]
S3_BUCKET           = os.environ["S3_BUCKET"]
MAX_ROUNDS          = int(os.environ.get("MAX_ROUNDS", 3))
SCORE_THRESHOLD     = float(os.environ.get("SCORE_THRESHOLD", 0.85))

RESEARCH_ANGLES = ["positive", "neutral", "negative"]


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
    return r



# DISPATCH RESEARCH TASKS
# Called for round 1 (no feedback) and round N (with feedback)

def dispatch_research_tasks(
    r: redis.Redis,
    debate_id: str,
    round_num: int,
    query: str,
    judge_feedback: dict = None,       # { angle: feedback_string } — None for round 1
    previous_findings: dict = None,    # { angle: findings_dict }   — None for round 1
):
    """
    Send one research task per angle to the research-tasks queue.
    For round > 1, includes judge_feedback and previous_findings so
    agents can improve on their prior work.
    """
    log(f"Dispatching research tasks — debate={debate_id} round={round_num}")

    for angle in RESEARCH_ANGLES:
        task = {
            "debate_id":          debate_id,
            "round":              round_num,
            "query":              query,
            "angle":              angle,
            "judge_feedback":     judge_feedback.get(angle, "") if judge_feedback else "",
            "previous_findings":  previous_findings.get(angle, {}) if previous_findings else {},
        }

        group_id = f"{debate_id}-{angle}-{round_num}"
        dedup_id = str(uuid.uuid4())

        sqs.send_message(
            QueueUrl=RESEARCH_QUEUE_URL,
            MessageBody=json.dumps(task),
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )
        ok(f"Research task sent — angle={angle} round={round_num}")

    # Mark round as started in Redis
    r.set(f"round_started:{debate_id}:{round_num}", "true", ex=86400)
    ok(f"All {len(RESEARCH_ANGLES)} research tasks dispatched for round {round_num}")



# INITIALIZE DEBATE STATE IN REDIS

def init_debate(r: redis.Redis, debate_id: str, query: str):
    """
    Store all debate metadata in Redis so every downstream
    lambda and agent can read it without it being passed
    through every message.
    """
    TTL = 86400  # 24 hours

    r.set(f"query:{debate_id}",        query,                     ex=TTL)
    r.set(f"status:{debate_id}",       "running",                 ex=TTL)
    r.set(f"current_round:{debate_id}", "1",                      ex=TTL)
    r.set(f"max_rounds:{debate_id}",   str(MAX_ROUNDS),           ex=TTL)
    r.set(f"threshold:{debate_id}",    str(SCORE_THRESHOLD),      ex=TTL)
    r.set(f"expected:{debate_id}",     str(len(RESEARCH_ANGLES)), ex=TTL)
    r.set(f"started_at:{debate_id}",   str(time.time()),          ex=TTL)

    ok(f"Debate state initialized in Redis — debate_id={debate_id}")



# GET RESULT (called by GET /debate/{debate_id}/result)

def get_result(r: redis.Redis, debate_id: str) -> dict:
    status = r.get(f"status:{debate_id}")
    if not status:
        return {"status": "not_found"}

    if status != "complete":
        current_round = r.get(f"current_round:{debate_id}") or "1"
        return {
            "status":        status,
            "current_round": int(current_round),
            "max_rounds":    MAX_ROUNDS,
        }

    # Debate complete — return final result
    final = r.get(f"final_result:{debate_id}")
    return {
        "status": "complete",
        "result": json.loads(final) if final else None,
    }



# MAIN HANDLER

def lambda_handler(event, context):
    print("\n" + "="*60)
    print("  ORCHESTRATOR LAMBDA")
    print("="*60)

    http_method = event.get("httpMethod", "POST")
    path        = event.get("path", "/debate")

    # ── CORS headers ─────────────────────────────────────────
    headers = {
        "Content-Type":                "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }

    # ── OPTIONS preflight ─────────────────────────────────────
    if http_method == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}

    try:
        r = get_redis()
    except Exception as e:
        fail(f"Redis connection failed: {e}")
        return {
            "statusCode": 503,
            "headers": headers,
            "body": json.dumps({"error": "Service unavailable — Redis unreachable"}),
        }

    # ── GET /debate/{debate_id}/result — poll for result ─────
    if http_method == "GET":
        path_parts = path.strip("/").split("/")
        # path: /debate/{debate_id}/result
        if len(path_parts) == 3 and path_parts[2] == "result":
            debate_id = path_parts[1]
            log(f"Polling result for debate_id={debate_id}")
            result = get_result(r, debate_id)
            return {
                "statusCode": 200,
                "headers": headers,
                "body": json.dumps(result),
            }
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"error": "Not found"}),
        }

    # ── POST /debate — start a new debate ────────────────────
    if http_method == "POST":
        try:
            body  = json.loads(event.get("body", "{}"))
            query = body.get("query", "").strip()
        except Exception:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Invalid JSON body"}),
            }

        if not query:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Missing required field: query"}),
            }

        if len(query) > 1000:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Query too long — max 1000 characters"}),
            }

        # Generate debate ID
        debate_id = f"D-{uuid.uuid4().hex[:12].upper()}"
        log(f"Starting new debate — id={debate_id}")
        log(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")

        # Initialize Redis state
        init_debate(r, debate_id, query)

        # Dispatch round 1 research tasks (no feedback yet)
        dispatch_research_tasks(
            r=r,
            debate_id=debate_id,
            round_num=1,
            query=query,
        )

        ok(f"Debate {debate_id} started — round 1 research tasks dispatched")

        return {
            "statusCode": 202,
            "headers": headers,
            "body": json.dumps({
                "debate_id":   debate_id,
                "status":      "running",
                "round":       1,
                "max_rounds":  MAX_ROUNDS,
                "message":     "Debate started. Poll GET /debate/{debate_id}/result for updates.",
            }),
        }

    return {
        "statusCode": 405,
        "headers": headers,
        "body": json.dumps({"error": "Method not allowed"}),
    }