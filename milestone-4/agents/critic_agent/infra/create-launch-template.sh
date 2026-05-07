#!/bin/bash
set -e

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
LAUNCH_TEMPLATE_NAME="critic-agent-template"
INSTANCE_TYPE="t3.micro"
AMI_ID="ami-02b9a589195146a8f"
SECURITY_GROUP_ID="sg-06b68dd650bc99451"
INSTANCE_PROFILE="LabInstanceProfile"
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")

echo "Using Launch Template: $LAUNCH_TEMPLATE_NAME"
echo "Instance Type:         $INSTANCE_TYPE"
echo "AMI ID:                $AMI_ID"
echo "Security Group:        $SECURITY_GROUP_ID"
echo "Instance Profile:      $INSTANCE_PROFILE"
echo "Region:                $REGION"


# ─────────────────────────────────────────────────────────────
# USER DATA SCRIPT
# ─────────────────────────────────────────────────────────────
cat > /tmp/user-data-critic.sh << 'OUTEREOF'
#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "Critic Agent Auto-Scaling Deployment"
echo "=========================================="

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
AGENT_ID="CA-${INSTANCE_ID}"

echo "Instance ID: $INSTANCE_ID"
echo "Region:      $REGION"
echo "Agent ID:    $AGENT_ID"

# -------------------------
# INSTALL DEPENDENCIES
# -------------------------
yum update -y
yum install -y python3-pip

mkdir -p /opt/critic-agent
cd /opt/critic-agent

pip3 install --upgrade pip
pip3 install boto3 openai python-dotenv

PYTHON_BIN=$(which python3)
echo "Python binary: $PYTHON_BIN"

# -------------------------
# INSTALL CLOUDWATCH AGENT
# -------------------------
echo "Installing CloudWatch agent..."
yum install -y amazon-cloudwatch-agent

cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << CWEOF
{
  "agent": {
    "run_as_user": "root"
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/user-data.log",
            "log_group_name": "/debate/critic-agent",
            "log_stream_name": "${INSTANCE_ID}/user-data",
            "timestamp_format": "%Y-%m-%d %H:%M:%S",
            "retention_in_days": 7
          },
          {
            "file_path": "/var/log/critic-agent.log",
            "log_group_name": "/debate/critic-agent",
            "log_stream_name": "${INSTANCE_ID}/app",
            "timestamp_format": "%Y-%m-%d %H:%M:%S",
            "retention_in_days": 7
          }
        ]
      }
    }
  }
}
CWEOF

/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s

echo "CloudWatch agent started."

# -------------------------
# WRITE CRITIC AGENT PYTHON SCRIPT
# -------------------------
cat > /opt/critic-agent/critic_agent.py << 'ENDOFPYTHON'
import os
import sys
import json
import uuid
import time
import logging
import threading
import urllib.request
import boto3
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/critic-agent.log"),
    ]
)
log = logging.getLogger("critic-agent")


def get_region():
    try:
        url = "http://169.254.169.254/latest/meta-data/placement/region"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.read().decode().strip()
    except Exception:
        return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


REGION   = get_region()
AGENT_ID = os.environ.get("CRITIC_AGENT_ID", "unknown")
log.info(f"Region: {REGION} | Agent: {AGENT_ID}")


def get_ssm(name, decrypt=False):
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    except Exception as e:
        log.error(f"SSM error [{name}]: {e}")
        return None


SQS_TASKS_QUEUE   = get_ssm('/critic-agent/tasks-queue-url')
SQS_RESULTS_QUEUE = get_ssm('/critic-agent/results-queue-url')
OPENAI_API_KEY    = get_ssm('/critic-agent/openai-api-key', decrypt=True)

if not all([SQS_TASKS_QUEUE, SQS_RESULTS_QUEUE, OPENAI_API_KEY]):
    log.error("Missing required SSM config — aborting.")
    sys.exit(1)

sqs = boto3.client('sqs', region_name=REGION)
llm = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────────────────────
# CRITIC LENS MAP
# Maps critic_slot number → critique lens.
# Each critic evaluates the full research bundle from a
# different critical angle, just like research agents each
# covered a different research angle.
#
#   Slot 1 — logical      → logical fallacies, reasoning gaps
#   Slot 2 — evidence     → source quality, validity of claims
#   Slot 3 — completeness → missing angles, blind spots
# ─────────────────────────────────────────────────────────────
CRITIC_LENS_MAP = {
    1: "logical",
    2: "evidence",
    3: "completeness",
}

CRITIC_SYSTEM_PROMPTS = {
    "logical": """You are a logical critic in a multi-agent debate system.
Your job is to evaluate the research bundle for logical fallacies, weak reasoning, and internal contradictions.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":              "logical",
  "overall_score":     <float 0.0-1.0>,
  "fallacies":         ["<fallacy 1>", "<fallacy 2>"],
  "contradictions":    ["<contradiction between angles if any>"],
  "weak_reasoning":    ["<weak argument 1>", "<weak argument 2>"],
  "strongest_angle":   "<which research angle had the strongest logic>",
  "weakest_angle":     "<which research angle had the weakest logic>",
  "recommendation":    "<overall recommendation to improve logical soundness>"
}
Be precise, rigorous, and cite specific claims from the research when identifying issues.""",

    "evidence": """You are an evidence critic in a multi-agent debate system.
Your job is to challenge the quality, validity, and reliability of sources and evidence cited.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":                "evidence",
  "overall_score":       <float 0.0-1.0>,
  "unsupported_claims":  ["<claim with no evidence>"],
  "weak_sources":        ["<source or evidence that is unreliable>"],
  "strong_evidence":     ["<evidence that is well supported>"],
  "missing_evidence":    ["<what evidence would strengthen the research>"],
  "strongest_angle":     "<which research angle had the best evidence>",
  "weakest_angle":       "<which research angle had the weakest evidence>",
  "recommendation":      "<overall recommendation to improve evidence quality>"
}
Be skeptical, demand high standards of evidence, and flag anything unverifiable.""",

    "completeness": """You are a completeness critic in a multi-agent debate system.
Your job is to identify what perspectives, angles, and considerations are entirely missing from the research.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":               "completeness",
  "overall_score":      <float 0.0-1.0>,
  "missing_angles":     ["<perspective not covered>", "<stakeholder not considered>"],
  "underexplored":      ["<topic mentioned but not explored enough>"],
  "blind_spots":        ["<assumption made without questioning>"],
  "suggested_research": ["<additional angle that should be researched>"],
  "coverage_summary":   "<brief summary of what was and was not covered>",
  "recommendation":     "<overall recommendation to improve completeness>"
}
Think broadly — consider all stakeholders, time horizons, and disciplines.""",
}


# ─────────────────────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────────────────────
VISIBILITY_TIMEOUT = 30
HEARTBEAT_INTERVAL = 20


def start_heartbeat(receipt: str) -> threading.Event:
    stop_event = threading.Event()

    def _beat():
        while not stop_event.wait(timeout=HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=SQS_TASKS_QUEUE,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                )
                log.info(f"Heartbeat — visibility renewed ({VISIBILITY_TIMEOUT}s)")
            except Exception as e:
                log.error(f"Heartbeat failed — {e}")
                break

    thread = threading.Thread(target=_beat, daemon=True, name="heartbeat")
    thread.start()
    log.info(f"Heartbeat started — interval={HEARTBEAT_INTERVAL}s timeout={VISIBILITY_TIMEOUT}s")
    return stop_event


# ─────────────────────────────────────────────────────────────
# CRITIQUE
# ─────────────────────────────────────────────────────────────
def critique(query: str, results: dict, lens: str) -> dict:
    """
    Send the full research bundle to GPT-4o for critique.
    results is a dict of { angle: findings } from all research agents.
    """
    system_prompt = CRITIC_SYSTEM_PROMPTS[lens]

    # Format research bundle clearly for the LLM
    research_text = ""
    for angle, findings in results.items():
        research_text += f"\n--- Angle: {angle.upper()} ---\n"
        research_text += json.dumps(findings, indent=2)
        research_text += "\n"

    user_content = (
        f"Query being debated: {query}\n\n"
        f"Research bundle to critique:\n{research_text}"
    )

    resp = llm.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_content},
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content)


# ─────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────
def loop():
    log.info(f"Critic agent {AGENT_ID} entering polling loop...")
    log.info(f"Visibility timeout: {VISIBILITY_TIMEOUT}s | Heartbeat interval: {HEARTBEAT_INTERVAL}s")

    while True:
        # ── Poll for one message ──────────────────────────────
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_TASKS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
            msgs = response.get("Messages", [])
        except Exception as e:
            log.error(f"Failed to poll SQS: {e}")
            time.sleep(5)
            continue

        for msg in msgs:
            receipt    = msg["ReceiptHandle"]
            message_id = msg["MessageId"]
            stop_event = None

            log.info(f"Received message — id={message_id}")

            try:
                # ── Parse task ───────────────────────────────────────
                body        = json.loads(msg["Body"])
                debate_id   = body["debate_id"]
                round_num   = int(body["round"])
                query       = body["query"]
                results     = body["results"]
                critic_slot = int(body["critic_slot"])

                # Map slot number to lens
                lens = CRITIC_LENS_MAP.get(critic_slot)
                if not lens:
                    log.error(f"Unknown critic_slot {critic_slot} — no lens mapped, skipping")
                    continue

                log.info(f"Task — debate={debate_id} round={round_num} slot={critic_slot} lens={lens!r}")
                log.info(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")
                log.info(f"Angles to critique: {list(results.keys())}")

                # ── Start heartbeat before slow GPT-4o call ──────────
                stop_event = start_heartbeat(receipt)

                # ── Critique ─────────────────────────────────────────
                log.info(f"Calling GPT-4o with lens={lens!r}...")
                critique_result = critique(query, results, lens)
                log.info(f"Critique complete — score={critique_result.get('overall_score', 'n/a')}")
                log.info(f"Critique details: {json.dumps(critique_result, indent=2)}")

                # ── Send result ──────────────────────────────────────
                result_body = json.dumps({
                    "debate_id":   debate_id,
                    "round":       round_num,
                    "critic_slot": critic_slot,
                    "lens":        lens,
                    "critique":    critique_result,
                    "agent_id":    AGENT_ID,
                    "timestamp":   time.time(),
                })

                group_id = f"{debate_id}-critic-{lens}-{round_num}"
                dedup_id = str(uuid.uuid4())

                sqs.send_message(
                    QueueUrl=SQS_RESULTS_QUEUE,
                    MessageBody=result_body,
                    MessageGroupId=group_id,
                    MessageDeduplicationId=dedup_id,
                )
                log.info(f"Critique result sent — debate={debate_id} round={round_num} lens={lens!r}")

            except KeyError as e:
                log.error(f"Malformed task message — missing field: {e}")

            except Exception as e:
                log.error(f"Error processing message {message_id}: {e}")

            finally:
                # ── Stop heartbeat ───────────────────────────────────
                if stop_event:
                    stop_event.set()
                    log.info("Heartbeat stopped")

                # ── Always delete the message ────────────────────────
                try:
                    sqs.delete_message(
                        QueueUrl=SQS_TASKS_QUEUE,
                        ReceiptHandle=receipt,
                    )
                    log.info(f"Message deleted — id={message_id}")
                except Exception as e:
                    log.error(f"Failed to delete message {message_id}: {e}")


if __name__ == "__main__":
    loop()
ENDOFPYTHON

chmod +x /opt/critic-agent/critic_agent.py

# -------------------------
# WRITE SYSTEMD SERVICE
# -------------------------
cat > /etc/systemd/system/critic-agent.service << SERVICEFILE
[Unit]
Description=Critic Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/critic-agent
Environment=CRITIC_AGENT_ID=${AGENT_ID}
ExecStart=${PYTHON_BIN} /opt/critic-agent/critic_agent.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/critic-agent.log
StandardError=append:/var/log/critic-agent.log

[Install]
WantedBy=multi-user.target
SERVICEFILE

# -------------------------
# ENABLE AND START SERVICE
# -------------------------
systemctl daemon-reload
systemctl enable critic-agent
systemctl start critic-agent

echo "=========================================="
echo "Deployment complete!"
echo "Agent ID:    ${AGENT_ID}"
echo "Python:      ${PYTHON_BIN}"
echo "Service:     $(systemctl is-active critic-agent)"
echo "=========================================="
OUTEREOF

chmod +x /tmp/user-data-critic.sh


# ─────────────────────────────────────────────────────────────
# BASE64 ENCODE USER DATA
# ─────────────────────────────────────────────────────────────
USER_DATA_B64=$(base64 -i /tmp/user-data-critic.sh | tr -d '\n')


# ─────────────────────────────────────────────────────────────
# DELETE EXISTING LAUNCH TEMPLATE IF IT EXISTS
# ─────────────────────────────────────────────────────────────
if aws ec2 describe-launch-templates \
     --region "$REGION" \
     --launch-template-names "$LAUNCH_TEMPLATE_NAME" \
     --query "LaunchTemplates[0].LaunchTemplateName" \
     --output text 2>/dev/null | grep -q "$LAUNCH_TEMPLATE_NAME"; then
  echo "Deleting existing launch template: $LAUNCH_TEMPLATE_NAME"
  aws ec2 delete-launch-template \
    --region "$REGION" \
    --launch-template-name "$LAUNCH_TEMPLATE_NAME"
fi


# ─────────────────────────────────────────────────────────────
# BUILD LAUNCH TEMPLATE JSON FILE
# ─────────────────────────────────────────────────────────────
cat > /tmp/critic-launch-template-data.json << LTEOF
{
  "NetworkInterfaces": [
    {
      "DeviceIndex": 0,
      "AssociatePublicIpAddress": false,
      "Groups": ["${SECURITY_GROUP_ID}"],
      "DeleteOnTermination": true
    }
  ],
  "ImageId": "${AMI_ID}",
  "InstanceType": "${INSTANCE_TYPE}",
  "IamInstanceProfile": {
    "Name": "${INSTANCE_PROFILE}"
  },
  "UserData": "${USER_DATA_B64}",
  "TagSpecifications": [
    {
      "ResourceType": "instance",
      "Tags": [
        {"Key": "Name", "Value": "critic-agent"},
        {"Key": "Role", "Value": "critic-agent"}
      ]
    }
  ]
}
LTEOF

aws ec2 create-launch-template \
  --region "$REGION" \
  --launch-template-name "$LAUNCH_TEMPLATE_NAME" \
  --version-description "v1-critic-agent" \
  --launch-template-data "file:///tmp/critic-launch-template-data.json"

echo ""
echo "✓ Launch template '$LAUNCH_TEMPLATE_NAME' created successfully."
echo ""
echo "  Critic lenses:"
echo "    Slot 1 — logical      → logical fallacies, reasoning gaps"
echo "    Slot 2 — evidence     → source quality, validity of claims"
echo "    Slot 3 — completeness → missing angles, blind spots"
echo ""
echo "  Logs → CloudWatch → Log Groups → /debate/critic-agent"
echo "    <instance-id>/user-data  — bootstrap & install logs"
echo "    <instance-id>/app        — critic agent log.info() output"
echo ""
echo "  Next: run create-critic-asg.sh to launch instances across both subnets."