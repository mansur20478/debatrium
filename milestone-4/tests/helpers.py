"""
helpers.py — Shared utilities for all failure-mode tests.
Loads config from debate_config.json and provides wrappers
for API calls, SQS, EC2, and result polling.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import boto3

# Add milestone-4/ to path so this module is importable as tests.helpers
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Config ────────────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "debate_config.json")
TOKEN_FILE  = os.path.expanduser("~/.debate_token")

def load_config() -> dict:
    with open(os.path.abspath(CONFIG_FILE)) as f:
        return json.load(f)

def load_token() -> str:
    if not os.path.exists(TOKEN_FILE):
        print("ERROR: Not logged in. Run: python3 auth/cli.py login ...")
        sys.exit(1)
    with open(TOKEN_FILE) as f:
        return json.load(f)["id_token"]

CFG = load_config()
REGION     = CFG["region"]
ACCOUNT_ID = CFG["account_id"]
API_URL    = CFG["api_gateway"]["url"].rstrip("/")
QUEUES     = CFG["queues"]
LAMBDA_SG  = CFG["security_groups"]["lambda_sg_id"]
REDIS_SG   = CFG["security_groups"]["elasticache_sg_id"]
S3_BUCKET  = CFG["bucket"]

# ── AWS clients ───────────────────────────────────────────────

ec2    = boto3.client("ec2",            region_name=REGION)
sqs    = boto3.client("sqs",            region_name=REGION)
s3     = boto3.client("s3",             region_name=REGION)
asg    = boto3.client("autoscaling",    region_name=REGION)
lamb   = boto3.client("lambda",         region_name=REGION)
cw     = boto3.client("cloudwatch",     region_name=REGION)
logs   = boto3.client("logs",           region_name=REGION)

# ── Pretty printing ───────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"

def section(title: str):
    print(f"\n{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{CYAN}{'─'*60}{RESET}")

def step(msg: str):
    print(f"  {DIM}→{RESET}  {msg}")

def ok(msg: str):
    print(f"  {GREEN}✓{RESET}  {msg}")

def warn(msg: str):
    print(f"  {YELLOW}⚠{RESET}  {msg}")

def fail(msg: str):
    print(f"  {RED}✗{RESET}  {msg}")

def result(passed: bool, msg: str):
    if passed:
        print(f"\n  {GREEN}{BOLD}PASS{RESET}  {msg}")
    else:
        print(f"\n  {RED}{BOLD}FAIL{RESET}  {msg}")
    return passed

# ── API helpers ───────────────────────────────────────────────

def start_debate(query: str, token: str) -> dict:
    data = json.dumps({"query": query}).encode()
    req  = urllib.request.Request(
        f"{API_URL}/debate",
        data=data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())

def poll_debate(debate_id: str, token: str) -> dict:
    req = urllib.request.Request(
        f"{API_URL}/debate/{debate_id}/result",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())

def wait_for_complete(debate_id: str, token: str, timeout: int = 600) -> dict:
    """Poll until debate completes or timeout (seconds). Returns final status dict."""
    start      = time.time()
    last_round = None
    while time.time() - start < timeout:
        resp   = poll_debate(debate_id, token)
        status = resp.get("status")
        if status == "complete":
            return resp
        if status == "not_found":
            return resp
        current = resp.get("current_round", "?")
        if current != last_round:
            step(f"Debate {debate_id} — round {current}/{resp.get('max_rounds','?')} running...")
            last_round = current
        time.sleep(10)
    return {"status": "timeout"}

# ── SQS helpers ───────────────────────────────────────────────

def queue_depth(queue_url: str) -> int:
    resp = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible"],
    )
    attrs = resp["Attributes"]
    return (int(attrs.get("ApproximateNumberOfMessages", 0)) +
            int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)))

def dlq_depth(queue_url: str) -> int:
    """Get DLQ depth for a given queue (looks for queue-name-dlq pattern)."""
    # Derive DLQ URL from main queue URL
    dlq_url = queue_url.replace(".fifo", "-dlq.fifo")
    try:
        resp = sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        return int(resp["Attributes"].get("ApproximateNumberOfMessages", 0))
    except Exception:
        return -1  # DLQ not found

# ── EC2 / ASG helpers ─────────────────────────────────────────

def get_agent_instances(role: str) -> list:
    """Return list of running EC2 instance IDs with the given Role tag."""
    resp = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Role",        "Values": [role]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    instances = []
    for r in resp["Reservations"]:
        for i in r["Instances"]:
            instances.append(i["InstanceId"])
    return instances

def get_asg_name(role: str) -> str | None:
    """Find an ASG whose instances carry the given Role tag."""
    resp = asg.describe_auto_scaling_groups()
    for group in resp["AutoScalingGroups"]:
        tags = {t["Key"]: t["Value"] for t in group.get("Tags", [])}
        if tags.get("Role") == role or tags.get("Name") == role:
            return group["AutoScalingGroupName"]
    return None

# ── Security group helpers ────────────────────────────────────

def block_redis():
    """Remove the Lambda→Redis inbound rule on port 6379."""
    ec2.revoke_security_group_ingress(
        GroupId=REDIS_SG,
        IpPermissions=[{
            "IpProtocol": "tcp",
            "FromPort":   6379,
            "ToPort":     6379,
            "UserIdGroupPairs": [{"GroupId": LAMBDA_SG}],
        }],
    )

def restore_redis():
    """Re-add the Lambda→Redis inbound rule on port 6379."""
    try:
        ec2.authorize_security_group_ingress(
            GroupId=REDIS_SG,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort":   6379,
                "ToPort":     6379,
                "UserIdGroupPairs": [{"GroupId": LAMBDA_SG}],
            }],
        )
    except Exception:
        pass  # already exists

# ── S3 helpers ────────────────────────────────────────────────

def remove_s3_policy():
    s3.delete_bucket_policy(Bucket=S3_BUCKET)

def restore_s3_policy():
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid":       "LambdaWrite",
            "Effect":    "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action":    ["s3:PutObject", "s3:GetObject"],
            "Resource":  f"arn:aws:s3:::{S3_BUCKET}/*",
        }],
    })
    s3.put_bucket_policy(Bucket=S3_BUCKET, Policy=policy)

# ── CloudWatch log helpers ────────────────────────────────────

def recent_lambda_errors(function_name: str, seconds: int = 120) -> list[str]:
    group = f"/aws/lambda/{function_name}"
    start = int((time.time() - seconds) * 1000)
    try:
        resp = logs.filter_log_events(
            logGroupName=group,
            startTime=start,
            filterPattern="ERROR",
        )
        return [e["message"].strip() for e in resp.get("events", [])]
    except Exception:
        return []
