"""
create_lambdas.py — Create all four Lambda functions for the debate system.

Uses the existing LabRole IAM role — no new roles created.
Reads VPC/subnet/queue config from debate_config.json.

Upload zip files to the same directory before running:
  aggregator_lambda.zip
  critic_aggregator_lambda.zip
  judge_aggregator_lambda.zip
  orchestrator_lambda.zip

Each zip must contain the .py file at the root:
  aggregator_lambda.zip
    └── aggregator_lambda.py
"""

import boto3
import json
import time
import sys
import os
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────────────────────
# LOAD CONFIG
# ─────────────────────────────────────────────────────────────
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "debate_config.json")

if not os.path.exists(os.path.abspath(CONFIG_FILE)):
    print(f"ERROR: debate_config.json not found. Run setup.py first.")
    sys.exit(1)

with open(os.path.abspath(CONFIG_FILE)) as f:
    cfg = json.load(f)

REGION             = cfg["region"]
ACCOUNT_ID         = cfg["account_id"]
PRIVATE_SUBNET_IDS = cfg["vpc"]["private_subnet_ids"]
LAMBDA_SG_ID       = cfg["security_groups"]["lambda_sg_id"]
QUEUES             = cfg["queues"]
REDIS_HOST         = cfg["redis"]["host"]
REDIS_PORT         = str(cfg["redis"]["port"])
S3_BUCKET          = cfg["bucket"]

# LabRole ARN — already exists in the account, no creation needed
LAB_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/LabRole"

# ─────────────────────────────────────────────────────────────
# LAMBDA DEFINITIONS
# ─────────────────────────────────────────────────────────────
LAMBDA_DEFINITIONS = {

    "aggregator_lambda": {
        "handler":     "aggregator_lambda.lambda_handler",
        "description": "Aggregates research results and dispatches to critics",
        "timeout":     300,
        "memory":      512,
        "zip_file":    "aggregator_lambda.zip",
        "env": {
            "REDIS_HOST":             REDIS_HOST,
            "REDIS_PORT":             REDIS_PORT,
            "CRITIC_TASKS_QUEUE_URL": QUEUES["critic_tasks"],
            "EXPECTED_RESULTS":       "3",
            "NUM_CRITIC_SLOTS":       "3",
        },
        "esm_queue_key": "research_results",
    },

    "critic_aggregator_lambda": {
        "handler":     "critic_aggregator_lambda.lambda_handler",
        "description": "Aggregates critic results and dispatches to judges",
        "timeout":     300,
        "memory":      512,
        "zip_file":    "critic_aggregator_lambda.zip",
        "env": {
            "REDIS_HOST":            REDIS_HOST,
            "REDIS_PORT":            REDIS_PORT,
            "JUDGE_TASKS_QUEUE_URL": QUEUES["judge_tasks"],
            "NUM_JUDGE_SLOTS":       "3",
        },
        "esm_queue_key": "critic_results",
    },

    "judge_aggregator_lambda": {
        "handler":     "judge_aggregator_lambda.lambda_handler",
        "description": "Aggregates judge verdicts — finalizes or starts next round",
        "timeout":     300,
        "memory":      512,
        "zip_file":    "judge_aggregator_lambda.zip",
        "env": {
            "REDIS_HOST":               REDIS_HOST,
            "REDIS_PORT":               REDIS_PORT,
            "RESEARCH_TASKS_QUEUE_URL": QUEUES["research_tasks"],
            "FINAL_RESULTS_QUEUE_URL":  QUEUES["final_results"],
            "S3_BUCKET":                S3_BUCKET,
            "MAX_ROUNDS":               "3",
            "SCORE_THRESHOLD":          "0.85",
        },
        "esm_queue_key": "judge_results",
    },

    "orchestrator_lambda": {
        "handler":     "orchestrator_lambda.lambda_handler",
        "description": "Debate orchestrator — triggered by API Gateway",
        "timeout":     29,
        "memory":      256,
        "zip_file":    "orchestrator_lambda.zip",
        "env": {
            "REDIS_HOST":               REDIS_HOST,
            "REDIS_PORT":               REDIS_PORT,
            "RESEARCH_TASKS_QUEUE_URL": QUEUES["research_tasks"],
            "S3_BUCKET":                S3_BUCKET,
            "MAX_ROUNDS":               "3",
            "SCORE_THRESHOLD":          "0.85",
        },
        "esm_queue_key": None,
    },
}

BATCH_SIZE      = 10
MAX_CONCURRENCY = 5

# ─────────────────────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────────────────────
lambda_ = boto3.client("lambda", region_name=REGION)
sqs     = boto3.client("sqs",    region_name=REGION)

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def log(msg):     print(f"  -> {msg}")
def ok(msg):      print(f"  OK {msg}")
def skip(msg):    print(f"  -- {msg}")
def warn(msg):    print(f"  !! {msg}")
def fail(msg):    print(f"  XX {msg}")
def section(msg): print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def wait_for_update(name: str, max_attempts: int = 80):
    """
    Poll until LastUpdateStatus is no longer InProgress.
    VPC attachment in particular can take 30-60 seconds.
    """
    for attempt in range(max_attempts):
        resp   = lambda_.get_function_configuration(FunctionName=name)
        status = resp.get("LastUpdateStatus", "Successful")
        state  = resp.get("State", "Active")
        if status == "Successful" and state == "Active":
            return
        if status == "Failed":
            fail(f"Update failed for {name}: {resp.get('LastUpdateStatusReasonCode')}")
            return
        print(f"    [{attempt+1}/{max_attempts}] State={state} UpdateStatus={status} — waiting...")
        time.sleep(5)
    warn(f"Timed out waiting for {name} — continuing anyway")


def wait_for_active(name: str, max_attempts: int = 40):
    """Poll until a newly created function reaches Active state."""
    log(f"Waiting for {name} to become Active...")
    for attempt in range(max_attempts):
        resp  = lambda_.get_function_configuration(FunctionName=name)
        state = resp.get("State", "")
        print(f"    [{attempt+1}/{max_attempts}] State: {state}")
        if state == "Active":
            return
        time.sleep(5)
    warn(f"{name} did not reach Active — check Lambda console")


# ─────────────────────────────────────────────────────────────
# CREATE OR UPDATE FUNCTION
# Strategy:
#   - If function doesn't exist: create WITHOUT VPC first (fast),
#     then update config to add VPC (VPC attachment is the slow part —
#     doing it separately means we can poll clearly and not hit timeouts)
#   - If function exists: update code first, then config
# ─────────────────────────────────────────────────────────────
def create_or_update_function(name: str, defn: dict) -> bool:
    zip_path = defn["zip_file"]

    if not os.path.exists(zip_path):
        fail(f"Zip not found: {zip_path} — skipping {name}")
        warn(f"Place {zip_path} in this directory and re-run.")
        return False

    with open(zip_path, "rb") as f:
        zip_bytes = f.read()

    vpc_config = {
        "SubnetIds":        PRIVATE_SUBNET_IDS,
        "SecurityGroupIds": [LAMBDA_SG_ID],
    }

    try:
        # ── Function already exists ───────────────────────────
        lambda_.get_function(FunctionName=name)
        skip(f"{name} already exists — updating")

        # 1. Update code
        log(f"Updating code for {name}...")
        lambda_.update_function_code(
            FunctionName=name,
            ZipFile=zip_bytes,
            Publish=False,
        )
        wait_for_update(name)
        ok(f"Code updated: {name}")

        # 2. Update config (handler, timeout, memory, env vars)
        log(f"Updating config for {name}...")
        lambda_.update_function_configuration(
            FunctionName=name,
            Handler=defn["handler"],
            Description=defn["description"],
            Timeout=defn["timeout"],
            MemorySize=defn["memory"],
            Environment={"Variables": defn["env"]},
        )
        wait_for_update(name)
        ok(f"Config updated: {name}")

        # 3. Update VPC separately — this is the slow step
        log(f"Updating VPC config for {name} (this takes ~30-60s)...")
        lambda_.update_function_configuration(
            FunctionName=name,
            VpcConfig=vpc_config,
        )
        wait_for_update(name)
        ok(f"VPC updated: {name}")
        return True

    except lambda_.exceptions.ResourceNotFoundException:
        # ── Function does not exist — create it ───────────────
        log(f"Creating {name} (no VPC first for speed)...")

        try:
            # Step 1: Create without VPC — this is instant
            lambda_.create_function(
                FunctionName=name,
                Runtime="python3.11",
                Role=LAB_ROLE_ARN,
                Handler=defn["handler"],
                Code={"ZipFile": zip_bytes},
                Description=defn["description"],
                Timeout=defn["timeout"],
                MemorySize=defn["memory"],
                Environment={"Variables": defn["env"]},
                PackageType="Zip",
                Tags={
                    "Project": "Debatrium",
                    "Role":    name,
                },
            )
            # Wait for Active before touching it again
            wait_for_active(name)
            ok(f"Function created: {name}")

            # Step 2: Attach VPC — separate call, clearly polled
            log(f"Attaching VPC to {name} (this takes ~30-60s)...")
            lambda_.update_function_configuration(
                FunctionName=name,
                VpcConfig=vpc_config,
            )
            wait_for_update(name)
            ok(f"VPC attached: {name}")
            ok(f"  Handler  : {defn['handler']}")
            ok(f"  Timeout  : {defn['timeout']}s | Memory: {defn['memory']}MB")
            ok(f"  Subnets  : {PRIVATE_SUBNET_IDS}")
            ok(f"  SG       : {LAMBDA_SG_ID}")
            return True

        except ClientError as e:
            fail(f"Failed to create {name}: {e}")
            return False


# ─────────────────────────────────────────────────────────────
# EVENT SOURCE MAPPINGS
# ─────────────────────────────────────────────────────────────
def setup_esm(name: str, queue_key: str):
    if queue_key not in QUEUES:
        warn(f"Queue key '{queue_key}' not in config -- skipping ESM for {name}")
        return

    queue_url = QUEUES[queue_key]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    # SQS requires: queue visibility timeout >= Lambda function timeout.
    # Our queues were created with 30s (for EC2 heartbeat agents) but
    # Lambda aggregators have 300s timeout. Update the queue before
    # creating the ESM, otherwise AWS rejects the mapping.
    log(f"Setting queue visibility timeout to 300s for {queue_key}...")
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"VisibilityTimeout": "300"},
    )
    ok(f"Queue visibility timeout updated: {queue_key}")

    log(f"ESM: {queue_key} -> {name}")

    # Check if ESM already exists
    paginator = lambda_.get_paginator("list_event_source_mappings")
    for page in paginator.paginate(FunctionName=name):
        for mapping in page["EventSourceMappings"]:
            if mapping["EventSourceArn"] == queue_arn:
                skip(f"ESM already exists: {mapping['UUID']} ({mapping['State']})")
                return

    try:
        resp     = lambda_.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=name,
            BatchSize=BATCH_SIZE,
            FunctionResponseTypes=["ReportBatchItemFailures"],
            ScalingConfig={"MaximumConcurrency": MAX_CONCURRENCY},
            Enabled=True,
        )
        esm_uuid = resp["UUID"]
        ok(f"ESM created: {esm_uuid}")

        # Poll until Enabled
        for _ in range(20):
            time.sleep(3)
            state = lambda_.get_event_source_mapping(UUID=esm_uuid)["State"]
            if state == "Enabled":
                ok(f"ESM active: {queue_key} -> {name}")
                return
            log(f"  ESM state: {state}")

        warn(f"ESM not yet Enabled — check Lambda console -> Triggers for {name}")

    except ClientError as e:
        fail(f"ESM creation failed for {name}: {e}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  DEBATE SYSTEM — Lambda Function Setup")
    print("="*60)
    print(f"\n  Region          : {REGION}")
    print(f"  Account         : {ACCOUNT_ID}")
    print(f"  IAM Role        : LabRole (existing)")
    print(f"  Private subnets : {PRIVATE_SUBNET_IDS}")
    print(f"  Security group  : {LAMBDA_SG_ID}")
    print(f"  Redis           : {REDIS_HOST}:{REDIS_PORT}")
    print()

    # Check zip files upfront so user knows what's missing
    print("  Zip file check:")
    all_present = True
    for name, defn in LAMBDA_DEFINITIONS.items():
        exists = os.path.exists(defn["zip_file"])
        status = "FOUND  " if exists else "MISSING"
        print(f"    {status} — {defn['zip_file']}")
        if not exists:
            all_present = False
    print()

    if not all_present:
        warn("Some zip files are missing — those functions will be skipped.")
        warn("Place missing zips in this directory and re-run.")
        print()

    # ── Create / update each function ────────────────────────
    section("Lambda Functions")
    created = []
    for name, defn in LAMBDA_DEFINITIONS.items():
        print(f"\n  [{name}]")
        success = create_or_update_function(name, defn)
        if success:
            created.append(name)

    # ── Wire ESMs ─────────────────────────────────────────────
    section("Event Source Mappings")
    for name, defn in LAMBDA_DEFINITIONS.items():
        if name not in created:
            warn(f"Skipping ESM for {name} — function was not created")
            continue
        queue_key = defn.get("esm_queue_key")
        if queue_key:
            setup_esm(name, queue_key)
        else:
            skip(f"No ESM for {name} — triggered by API Gateway")

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "="*60)
    print("  COMPLETE")
    print("="*60)
    for name, defn in LAMBDA_DEFINITIONS.items():
        status = "OK     " if name in created else "SKIPPED"
        print(f"  {status} {name}")
        print(f"           handler={defn['handler']}")
        print(f"           timeout={defn['timeout']}s  memory={defn['memory']}MB")
        print(f"           esm={defn['esm_queue_key'] or 'none (API Gateway)'}")
    print()
    print("  Next: create API Gateway -> orchestrator_lambda")
    print("="*60 + "\n")


def esm_only():
    """
    Run only the ESM step — use this when lambdas are already
    created and you just need to wire the queues.
    Fixes the visibility timeout on each queue before creating the ESM.
    """
    print("\n" + "="*60)
    print("  ESM ONLY MODE")
    print("="*60)
    for name, defn in LAMBDA_DEFINITIONS.items():
        queue_key = defn.get("esm_queue_key")
        if queue_key:
            print(f"\n  [{name}]")
            setup_esm(name, queue_key)
        else:
            skip(f"No ESM for {name} (API Gateway triggered)")
    print("\nDone.\n")


if __name__ == "__main__":
    # Change to esm_only() if lambdas already exist and
    # you only need to fix the ESM queue wiring.
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--esm-only":
        esm_only()
    else:
        main()