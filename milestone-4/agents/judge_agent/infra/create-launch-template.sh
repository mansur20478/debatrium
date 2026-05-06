#!/bin/bash
set -e

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
LAUNCH_TEMPLATE_NAME="judge-agent-template"
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
cat > /tmp/user-data-judge.sh << 'OUTEREOF'
#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "Judge Agent Auto-Scaling Deployment"
echo "=========================================="

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
AGENT_ID="JA-${INSTANCE_ID}"

echo "Instance ID: $INSTANCE_ID"
echo "Region:      $REGION"
echo "Agent ID:    $AGENT_ID"

# -------------------------
# INSTALL DEPENDENCIES
# -------------------------
yum update -y
yum install -y python3-pip

mkdir -p /opt/judge-agent
cd /opt/judge-agent

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
  "agent": { "run_as_user": "root" },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/user-data.log",
            "log_group_name": "/debate/judge-agent",
            "log_stream_name": "${INSTANCE_ID}/user-data",
            "timestamp_format": "%Y-%m-%d %H:%M:%S",
            "retention_in_days": 7
          },
          {
            "file_path": "/var/log/judge-agent.log",
            "log_group_name": "/debate/judge-agent",
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
# WRITE JUDGE AGENT PYTHON SCRIPT
# -------------------------
cat > /opt/judge-agent/judge_agent.py << 'ENDOFPYTHON'
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
        logging.FileHandler("/var/log/judge-agent.log"),
    ]
)
log = logging.getLogger("judge-agent")


def get_region():
    try:
        url = "http://169.254.169.254/latest/meta-data/placement/region"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.read().decode().strip()
    except Exception:
        return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


REGION   = get_region()
AGENT_ID = os.environ.get("JUDGE_AGENT_ID", "unknown")
log.info(f"Region: {REGION} | Agent: {AGENT_ID}")


def get_ssm(name, decrypt=False):
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    except Exception as e:
        log.error(f"SSM error [{name}]: {e}")
        return None


SQS_TASKS_QUEUE   = get_ssm('/judge-agent/tasks-queue-url')
SQS_RESULTS_QUEUE = get_ssm('/judge-agent/results-queue-url')
OPENAI_API_KEY    = get_ssm('/judge-agent/openai-api-key', decrypt=True)

if not all([SQS_TASKS_QUEUE, SQS_RESULTS_QUEUE, OPENAI_API_KEY]):
    log.error("Missing required SSM config — aborting.")
    sys.exit(1)

sqs = boto3.client('sqs', region_name=REGION)
llm = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────────────────────
# JUDGE LENS MAP
# Slot 1 — fairness    → balanced treatment of all angles
# Slot 2 — accuracy    → factual correctness of claims
# Slot 3 — consensus   → where angles agree, strength of debate
# ─────────────────────────────────────────────────────────────
JUDGE_LENS_MAP = {
    1: "fairness",
    2: "accuracy",
    3: "consensus",
}

JUDGE_SYSTEM_PROMPTS = {
    "fairness": """You are a fairness judge in a multi-agent debate system.
Evaluate whether all research angles were treated with equal rigor and without bias.
You receive the full research bundle AND critique bundle.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":                    "fairness",
  "overall_score":           <float 0.0-1.0>,
  "per_angle_scores": {
    "positive":              <float 0.0-1.0>,
    "neutral":               <float 0.0-1.0>,
    "negative":              <float 0.0-1.0>
  },
  "bias_detected":           ["<bias 1>", "<bias 2>"],
  "underrepresented_angles": ["<angle that got less attention>"],
  "feedback_for_next_round": {
    "positive":              "<specific improvement needed>",
    "neutral":               "<specific improvement needed>",
    "negative":              "<specific improvement needed>"
  },
  "round_verdict":           "continue | finalize",
  "summary":                 "<2-3 sentence overall fairness assessment>"
}""",

    "accuracy": """You are an accuracy judge in a multi-agent debate system.
Evaluate the factual correctness of all research angles and whether critiques correctly identified errors.
You receive the full research bundle AND critique bundle.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":                    "accuracy",
  "overall_score":           <float 0.0-1.0>,
  "per_angle_scores": {
    "positive":              <float 0.0-1.0>,
    "neutral":               <float 0.0-1.0>,
    "negative":              <float 0.0-1.0>
  },
  "factual_errors":          ["<error 1>", "<error 2>"],
  "well_supported_claims":   ["<claim 1>", "<claim 2>"],
  "feedback_for_next_round": {
    "positive":              "<specific improvement needed>",
    "neutral":               "<specific improvement needed>",
    "negative":              "<specific improvement needed>"
  },
  "round_verdict":           "continue | finalize",
  "summary":                 "<2-3 sentence overall accuracy assessment>"
}""",

    "consensus": """You are a consensus judge in a multi-agent debate system.
Identify where research angles agree, where they diverge, and how strong the overall debate is.
You receive the full research bundle AND critique bundle.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "lens":                    "consensus",
  "overall_score":           <float 0.0-1.0>,
  "per_angle_scores": {
    "positive":              <float 0.0-1.0>,
    "neutral":               <float 0.0-1.0>,
    "negative":              <float 0.0-1.0>
  },
  "points_of_agreement":     ["<shared finding 1>", "<shared finding 2>"],
  "points_of_disagreement":  ["<divergence 1>", "<divergence 2>"],
  "strongest_angle":         "<angle with most convincing overall argument>",
  "feedback_for_next_round": {
    "positive":              "<specific improvement needed>",
    "neutral":               "<specific improvement needed>",
    "negative":              "<specific improvement needed>"
  },
  "round_verdict":           "continue | finalize",
  "summary":                 "<2-3 sentence overall consensus assessment>"
}""",
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
# JUDGE
# ─────────────────────────────────────────────────────────────
def judge(query: str, research: dict, critiques: dict, lens: str) -> dict:
    system_prompt = JUDGE_SYSTEM_PROMPTS[lens]

    research_text  = "\n=== RESEARCH BUNDLE ===\n"
    for angle, findings in research.items():
        research_text += f"\n--- Angle: {angle.upper()} ---\n"
        research_text += json.dumps(findings, indent=2) + "\n"

    critique_text  = "\n=== CRITIQUE BUNDLE ===\n"
    for clens, critique in critiques.items():
        critique_text += f"\n--- Critic Lens: {clens.upper()} ---\n"
        critique_text += json.dumps(critique, indent=2) + "\n"

    user_content = (
        f"Query being debated: {query}\n"
        f"{research_text}"
        f"{critique_text}"
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
    log.info(f"Judge agent {AGENT_ID} entering polling loop...")

    while True:
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
                # ── Parse ────────────────────────────────────────────
                body       = json.loads(msg["Body"])
                debate_id  = body["debate_id"]
                round_num  = int(body["round"])
                query      = body["query"]
                research   = body["research"]
                critiques  = body["critiques"]
                judge_slot = int(body["judge_slot"])

                lens = JUDGE_LENS_MAP.get(judge_slot)
                if not lens:
                    log.error(f"Unknown judge_slot {judge_slot} — skipping")
                    continue

                log.info(f"Task — debate={debate_id} round={round_num} slot={judge_slot} lens={lens!r}")

                # ── Heartbeat ────────────────────────────────────────
                stop_event = start_heartbeat(receipt)

                # ── Judge ────────────────────────────────────────────
                log.info(f"Calling GPT-4o with lens={lens!r}...")
                verdict = judge(query, research, critiques, lens)
                log.info(f"Verdict complete — score={verdict.get('overall_score', 'n/a')} verdict={verdict.get('round_verdict', 'n/a')}")
                log.info(f"Verdict details: {json.dumps(verdict, indent=2)}")

                # ── Send result ──────────────────────────────────────
                result_body = json.dumps({
                    "debate_id":  debate_id,
                    "round":      round_num,
                    "judge_slot": judge_slot,
                    "lens":       lens,
                    "verdict":    verdict,
                    "agent_id":   AGENT_ID,
                    "timestamp":  time.time(),
                })

                group_id = f"{debate_id}-judge-{lens}-{round_num}"
                dedup_id = str(uuid.uuid4())

                sqs.send_message(
                    QueueUrl=SQS_RESULTS_QUEUE,
                    MessageBody=result_body,
                    MessageGroupId=group_id,
                    MessageDeduplicationId=dedup_id,
                )
                log.info(f"Verdict sent — debate={debate_id} round={round_num} lens={lens!r}")

            except KeyError as e:
                log.error(f"Malformed task — missing field: {e}")

            except Exception as e:
                log.error(f"Error processing message {message_id}: {e}")

            finally:
                if stop_event:
                    stop_event.set()
                    log.info("Heartbeat stopped")

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

chmod +x /opt/judge-agent/judge_agent.py

# -------------------------
# WRITE SYSTEMD SERVICE
# -------------------------
cat > /etc/systemd/system/judge-agent.service << SERVICEFILE
[Unit]
Description=Judge Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/judge-agent
Environment=JUDGE_AGENT_ID=${AGENT_ID}
ExecStart=${PYTHON_BIN} /opt/judge-agent/judge_agent.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/judge-agent.log
StandardError=append:/var/log/judge-agent.log

[Install]
WantedBy=multi-user.target
SERVICEFILE

systemctl daemon-reload
systemctl enable judge-agent
systemctl start judge-agent

echo "=========================================="
echo "Deployment complete!"
echo "Agent ID:    ${AGENT_ID}"
echo "Python:      ${PYTHON_BIN}"
echo "Service:     $(systemctl is-active judge-agent)"
echo "=========================================="
OUTEREOF

chmod +x /tmp/user-data-judge.sh

USER_DATA_B64=$(base64 -i /tmp/user-data-judge.sh | tr -d '\n')

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

cat > /tmp/judge-launch-template-data.json << LTEOF
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
  "IamInstanceProfile": { "Name": "${INSTANCE_PROFILE}" },
  "UserData": "${USER_DATA_B64}",
  "TagSpecifications": [
    {
      "ResourceType": "instance",
      "Tags": [
        {"Key": "Name", "Value": "judge-agent"},
        {"Key": "Role", "Value": "judge-agent"}
      ]
    }
  ]
}
LTEOF

aws ec2 create-launch-template \
  --region "$REGION" \
  --launch-template-name "$LAUNCH_TEMPLATE_NAME" \
  --version-description "v1-judge-agent" \
  --launch-template-data "file:///tmp/judge-launch-template-data.json"

echo ""
echo "✓ Launch template '$LAUNCH_TEMPLATE_NAME' created successfully."
echo ""
echo "  Judge lenses:"
echo "    Slot 1 — fairness   → balanced treatment of all angles"
echo "    Slot 2 — accuracy   → factual correctness of claims"
echo "    Slot 3 — consensus  → agreement strength across angles"
echo ""
echo "  Logs → CloudWatch → Log Groups → /debate/judge-agent"
echo "  Next: run create-judge-asg.sh"