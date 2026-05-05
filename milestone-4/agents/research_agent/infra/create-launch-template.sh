#!/bin/bash
set -e

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
LAUNCH_TEMPLATE_NAME="research-agent-template"
INSTANCE_TYPE="t3.micro"
AMI_ID="ami-02b9a589195146a8f"
SECURITY_GROUP_ID="sg-0801378588349451b"
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
cat > /tmp/user-data.sh << 'OUTEREOF'
#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "Research Agent Auto-Scaling Deployment"
echo "=========================================="

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
AGENT_ID="RA-${INSTANCE_ID}"

echo "Instance ID: $INSTANCE_ID"
echo "Region:      $REGION"
echo "Agent ID:    $AGENT_ID"

# -------------------------
# INSTALL DEPENDENCIES
# -------------------------
yum update -y
yum install -y python3-pip

mkdir -p /opt/research-agent
cd /opt/research-agent

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
            "log_group_name": "/debate/research-agent",
            "log_stream_name": "${INSTANCE_ID}/user-data",
            "timestamp_format": "%Y-%m-%d %H:%M:%S",
            "retention_in_days": 7
          },
          {
            "file_path": "/var/log/research-agent.log",
            "log_group_name": "/debate/research-agent",
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
# WRITE RESEARCH AGENT PYTHON SCRIPT
# -------------------------
cat > /opt/research-agent/research_agent.py << 'ENDOFPYTHON'
import os
import sys
import json
import time
import uuid
import logging
import hashlib
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
        logging.FileHandler("/var/log/research-agent.log"),
    ]
)
log = logging.getLogger("research-agent")


def get_region():
    try:
        url = "http://169.254.169.254/latest/meta-data/placement/region"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.read().decode().strip()
    except Exception:
        return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


REGION   = get_region()
AGENT_ID = os.environ.get("RESEARCH_AGENT_ID", "unknown")
log.info(f"Region: {REGION} | Agent: {AGENT_ID}")


def get_ssm(name, decrypt=False):
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    except Exception as e:
        log.error(f"SSM error [{name}]: {e}")
        return None


SQS_TASKS_QUEUE   = get_ssm('/research-agent/tasks-queue-url')
SQS_RESULTS_QUEUE = get_ssm('/research-agent/results-queue-url')
OPENAI_API_KEY    = get_ssm('/research-agent/openai-api-key', decrypt=True)

if not all([SQS_TASKS_QUEUE, SQS_RESULTS_QUEUE, OPENAI_API_KEY]):
    log.error("Missing required SSM config — aborting.")
    sys.exit(1)

sqs = boto3.client('sqs', region_name=REGION)
llm = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────────────────────
# HEARTBEAT
# Keeps the message invisible to other workers while this
# instance is alive and processing. If this instance crashes,
# the heartbeat thread dies with it — visibility timeout
# expires in at most VISIBILITY_TIMEOUT seconds, and another
# healthy worker picks up the message automatically.
#
# Timeline when instance is alive:
#   t=0   receive message, visibility=VISIBILITY_TIMEOUT (30s)
#   t=20  heartbeat fires, resets visibility to 30s
#   t=40  heartbeat fires, resets visibility to 30s
#   ...   continues until work is done
#
# Timeline when instance crashes at t=25:
#   t=0   receive message, visibility=30s
#   t=20  last heartbeat fires
#   t=25  CRASH — heartbeat thread dies
#   t=50  visibility expires (30s after last heartbeat)
#   t=51  another worker picks up the message ✓
#
# HEARTBEAT_INTERVAL must always be less than VISIBILITY_TIMEOUT
# to avoid a race where the timeout expires before the next renewal.
# ─────────────────────────────────────────────────────────────
VISIBILITY_TIMEOUT  = 30   # seconds — kept short so crashes recover fast
HEARTBEAT_INTERVAL  = 20   # seconds — must be < VISIBILITY_TIMEOUT


def start_heartbeat(receipt: str) -> threading.Event:
    """
    Spawn a background daemon thread that renews the visibility
    timeout every HEARTBEAT_INTERVAL seconds.

    Returns a stop_event — call stop_event.set() to kill the thread
    cleanly when processing is done.
    """
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
                break  # stop trying if SQS rejects (e.g. message already deleted)

    thread = threading.Thread(target=_beat, daemon=True, name="heartbeat")
    thread.start()
    log.info(f"Heartbeat started — interval={HEARTBEAT_INTERVAL}s timeout={VISIBILITY_TIMEOUT}s")
    return stop_event


# ─────────────────────────────────────────────────────────────
# RESEARCH
# ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a specialized research agent in a multi-agent debate system.
Your job is to research a specific angle of a query thoroughly and objectively.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "summary":     "<3-5 sentence summary of key findings>",
  "key_points":  ["<point 1>", "<point 2>", "<point 3>"],
  "evidence":    ["<evidence/source 1>", "<evidence/source 2>"],
  "confidence":  <float 0.0-1.0>,
  "limitations": "<what this angle misses or cannot address>"
}
Be factual, cite real evidence, and be honest about confidence levels."""


def research(query: str, angle: str) -> dict:
    resp = llm.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Query: {query}\nAngle to research: {angle}"}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content)


# ─────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────
def loop():
    log.info(f"Agent {AGENT_ID} entering polling loop...")
    log.info(f"Visibility timeout: {VISIBILITY_TIMEOUT}s | Heartbeat interval: {HEARTBEAT_INTERVAL}s")

    while True:
        # ── Poll for one message ──────────────────────────────
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_TASKS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=VISIBILITY_TIMEOUT,  # start short — heartbeat keeps it alive
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
                body      = json.loads(msg["Body"])
                query     = body["query"]
                angle     = body["angle"]
                debate_id = body["debate_id"]
                round_num = int(body["round"])

                log.info(f"Task — debate={debate_id} round={round_num} angle={angle!r}")
                log.info(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")

                # ── Start heartbeat before the slow GPT-4o call ──────
                stop_event = start_heartbeat(receipt)

                # ── Research ─────────────────────────────────────────
                log.info("Calling GPT-4o...")
                findings = research(query, angle)
                log.info(f"Research complete — confidence={findings.get('confidence', 'n/a')}")

                # ── Send result ──────────────────────────────────────
                result_body = json.dumps({
                    "debate_id": debate_id,
                    "round":     round_num,
                    "angle":     angle,
                    "findings":  findings,
                })

                # One group per task — no blocking between messages
                group_id = f"{debate_id}-{angle}-{round_num}"
                dedup_id = str(uuid.uuid4())

                sqs.send_message(
                    QueueUrl=SQS_RESULTS_QUEUE,
                    MessageBody=result_body,
                    MessageGroupId=group_id,
                    MessageDeduplicationId=dedup_id,
                )
                log.info(f"Result sent — debate={debate_id} round={round_num} angle={angle!r}")

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
                # Deletes on both success and handled failure so the
                # message doesn't get redelivered for a bad payload.
                # Unhandled crashes won't reach here — visibility
                # timeout expires and another worker retries.
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

chmod +x /opt/research-agent/research_agent.py

# -------------------------
# WRITE SYSTEMD SERVICE
# stdout → /var/log/research-agent.log (tailed by CloudWatch agent)
# -------------------------
cat > /etc/systemd/system/research-agent.service << SERVICEFILE
[Unit]
Description=Research Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/research-agent
Environment=RESEARCH_AGENT_ID=${AGENT_ID}
ExecStart=${PYTHON_BIN} /opt/research-agent/research_agent.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/research-agent.log
StandardError=append:/var/log/research-agent.log

[Install]
WantedBy=multi-user.target
SERVICEFILE

# -------------------------
# ENABLE AND START SERVICE
# -------------------------
systemctl daemon-reload
systemctl enable research-agent
systemctl start research-agent

echo "=========================================="
echo "Deployment complete!"
echo "Agent ID:    ${AGENT_ID}"
echo "Python:      ${PYTHON_BIN}"
echo "Service:     $(systemctl is-active research-agent)"
echo "=========================================="
OUTEREOF

chmod +x /tmp/user-data.sh


# ─────────────────────────────────────────────────────────────
# BASE64 ENCODE USER DATA
# ─────────────────────────────────────────────────────────────
USER_DATA_B64=$(base64 -i /tmp/user-data.sh | tr -d '\n')


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
cat > /tmp/launch-template-data.json << LTEOF
{
  "NetworkInterfaces": [
    {
      "DeviceIndex": 0,
      "AssociatePublicIpAddress": true,
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
        {"Key": "Name", "Value": "research-agent"},
        {"Key": "Role", "Value": "research-agent"}
      ]
    }
  ]
}
LTEOF

aws ec2 create-launch-template \
  --region "$REGION" \
  --launch-template-name "$LAUNCH_TEMPLATE_NAME" \
  --version-description "v8-heartbeat" \
  --launch-template-data "file:///tmp/launch-template-data.json"

echo ""
echo "✓ Launch template '$LAUNCH_TEMPLATE_NAME' created successfully."
echo ""
echo "  Heartbeat config:"
echo "    Visibility timeout : ${VISIBILITY_TIMEOUT}s"
echo "    Heartbeat interval : ${HEARTBEAT_INTERVAL}s"
echo "    Max crash recovery : ~${VISIBILITY_TIMEOUT}s after last heartbeat"
echo ""
echo "  Logs → CloudWatch → Log Groups → /debate/research-agent"
echo "    <instance-id>/user-data  — bootstrap & install logs"
echo "    <instance-id>/app        — research agent log.info() output"
echo ""
echo "  Next: run create-asg.sh to launch instances across both subnets."