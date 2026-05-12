# Debatrium — Distributed Multi-Agent Debate System

A fault-tolerant distributed system that answers user questions by orchestrating multiple AI agents that **research**, **critique**, and **judge** different angles of a debate in parallel rounds, then synthesizes a verdict.

Built for CSCI 6421 — Distributed Systems.

## Team

| Name |
|---|
| Alekya Kowta (Team Leader) |
| Eyouel Kibret |
| Mansur Mukimbekov |
| Michael Womack |

---

## Architecture

```
  User (UI / CLI)
        |
        v
  API Gateway  --[Firebase token authorizer]--> orchestrator_lambda
                                                       |
                                                       v
                          [SQS] research-tasks <--- writes initial state to Redis
                              |
                              v
                  Research EC2 Agents (x3 angles: positive, neutral, negative)
                              |
                              v
                          [SQS] research-results
                              |
                              v
                  aggregator_lambda  --(when 3/3 received)--> fan out to critics
                              |
                              v
                          [SQS] critic-tasks
                              |
                              v
                  Critic EC2 Agents  (x3 lenses: logical, evidence, completeness)
                              |
                              v
                          [SQS] critic-results
                              |
                              v
                  critic_aggregator_lambda --(when 3/3)--> fan out to judges
                              |
                              v
                          [SQS] judge-tasks
                              |
                              v
                  Judge EC2 Agents   (x3 lenses: fairness, accuracy, consensus)
                              |
                              v
                          [SQS] judge-results
                              |
                              v
                  judge_aggregator_lambda
                     |                        |
                     |  (score < 0.85)         (score >= 0.85 OR round == 3)
                     v                        v
                start round N+1         write final result to S3
                                                  |
                                                  v
                                          UI polls /result endpoint
```

### Key properties

- **Stateful coordination via Redis** — orchestrator and aggregators write per-debate state (rounds, expected counts, scores).
- **FIFO SQS queues with DLQs** — each task and result queue has a dead-letter queue with `maxReceiveCount: 3`.
- **Heartbeat-based crash recovery** — each EC2 agent renews SQS visibility every 20s while processing. If the agent crashes, the message reappears after ~30s and another worker picks it up.
- **At-least-once delivery, idempotent processing** — `MessageDeduplicationId` on every SQS send prevents duplicates.
- **Auto-scaling EC2 agent pools** — each agent role lives in its own ASG; terminated instances are auto-replaced.

---

## Folder Structure

```
final-project-albert-einstein/
├── README.md                              <- you are here
├── milestone-1/                           <- early design & docs
├── milestone-2/                           <- second milestone
├── milestone-3/                           <- third milestone
└── milestone-4/                           <- final implementation
    ├── HOW_TO_RUN.md                      <- detailed setup guide (phases 1-7)
    ├── debate_config.json                 <- generated; holds all AWS resource IDs
    ├── requirements.txt                   <- Python deps for local CLI + tests
    │
    ├── infra/                             <- AWS infrastructure setup scripts
    │   ├── setup.py                       <- VPC, SQS, S3, ElastiCache, SSM
    │   ├── setup-firebase.py              <- saves Firebase config to debate_config.json
    │   ├── create-api-auth.py             <- deploys Firebase authorizer Lambda
    │   ├── deploy-ui.py                   <- uploads UI to S3 static website bucket
    │   └── debate_config.json             <- mirror of root config
    │
    ├── lambdas/                           <- Lambda function source + deploy script
    │   ├── create-lambdas.py              <- creates 4 lambdas + SQS event mappings
    │   ├── orchestrator/                  <- API Gateway entry point
    │   ├── aggregator/                    <- research aggregator
    │   ├── dispatcher/                    <- critic aggregator
    │   ├── judge-aggregator/              <- judge aggregator (writes final result)
    │   ├── firebase-authorizer/           <- TOKEN authorizer for API Gateway
    │   └── *.zip                          <- pre-built deployment artifacts
    │
    ├── agents/                            <- EC2 agent code (baked into user-data)
    │   ├── research_agent/infra/
    │   │   ├── create-launch-template.sh  <- creates LT with research_agent.py inline
    │   │   └── create-asg.sh              <- creates the ASG
    │   ├── critic_agent/infra/
    │   │   ├── create-launch-template.sh
    │   │   └── create-asg.sh
    │   └── judge_agent/infra/
    │       ├── create-launch-template.sh
    │       └── create-asg.sh
    │
    ├── auth/
    │   └── cli.py                         <- Firebase-authenticated debate CLI
    │
    ├── ui/                                <- static web UI (HTML/CSS/JS)
    │   ├── index.html                     <- login page
    │   ├── dashboard.html                 <- "My Debates" list
    │   ├── debate.html                    <- new debate + live results
    │   ├── app.js                         <- Firebase auth + Firestore helpers
    │   ├── firebase-config.js             <- generated by deploy-ui.py
    │   └── styles.css
    │
    └── tests/                             <- 7 distributed-systems failure tests
        ├── run_all.py                     <- test runner with summary
        ├── helpers.py                     <- shared test utilities
        ├── test_1_agent_crash.py          <- terminates an agent mid-debate
        ├── test_2_redis_partition.py      <- blocks Lambda -> Redis traffic
        ├── test_3_partial_agents.py       <- 1 agent has to do all 3 angles
        ├── test_4_poison_message.py       <- malformed payload in queue
        ├── test_5_deduplication.py        <- duplicate SQS sends
        ├── test_6_concurrent_debates.py   <- 3 debates in parallel
        └── test_7_s3_failure.py           <- final write fails
```

---

## Setup — How to Run from Scratch

All commands run from `milestone-4/` unless stated. For full details and lab-reset recovery, see `milestone-4/HOW_TO_RUN.md`.

### Prerequisites

- AWS Academy lab session active (or AWS account with admin permissions)
- Python 3.9+
- A Firebase project with Email/Password auth + Firestore enabled (see Phase 5 below)
- An OpenAI API key (or NVIDIA NIM key — agents support both)

### Phase 1 — Core AWS infrastructure

```bash
cd milestone-4
pip3 install -r requirements.txt
python3 infra/setup.py
```

Creates VPC, public/private subnets, NAT gateway, security groups, all SQS FIFO queues + DLQs, S3 results bucket, ElastiCache Redis cluster, and SSM parameters. Prompts for your OpenAI/NVIDIA API key — paste it once and it gets written to three SSM paths (`/research-agent/openai-api-key`, `/critic-agent/openai-api-key`, `/judge-agent/openai-api-key`).

> **Validate the key first** before pasting:
> ```bash
> read -s KEY    # paste, Enter (input hidden)
> curl -s https://api.openai.com/v1/models -H "Authorization: Bearer $KEY" | head -c 200
> ```
> If it returns `"invalid_api_key"`, get a different key — don't waste a setup run on a bad key.

### Phase 2 — Lambda functions

```bash
python3 lambdas/create-lambdas.py
```

Creates `orchestrator_lambda`, `aggregator_lambda`, `critic_aggregator_lambda`, `judge_aggregator_lambda` and wires their SQS event source mappings.

### Phase 3 — API Gateway (AWS Console, manual)

In the AWS Console → API Gateway → Create REST API. Add resources `/debate` (POST) and `/debate/{debate_id}/result` (GET), each integrated with `orchestrator_lambda`. Enable CORS. Deploy to a stage named `prod`. Copy the Invoke URL.

Add to `debate_config.json`:
```json
"api_gateway": {
  "rest_api_id": "<id>",
  "api_name":    "debate-api",
  "stage":       "prod",
  "url":         "https://<id>.execute-api.us-east-1.amazonaws.com/prod"
}
```

### Phase 4 — Agent launch templates + ASGs

```bash
bash agents/research_agent/infra/create-launch-template.sh
bash agents/critic_agent/infra/create-launch-template.sh
bash agents/judge_agent/infra/create-launch-template.sh

bash agents/research_agent/infra/create-asg.sh
bash agents/critic_agent/infra/create-asg.sh
bash agents/judge_agent/infra/create-asg.sh
```

Spins up 2 instances per role (6 total). Each instance:
- Reads its config from SSM
- Polls its SQS task queue
- Sends results to its SQS results queue
- Streams logs to CloudWatch (`/debate/research-agent`, etc.)

### Phase 5 — Firebase (Console, one-time)

In the [Firebase Console](https://console.firebase.google.com):

1. Create a project.
2. Authentication → Sign-in method → enable **Email/Password**.
3. Firestore Database → Create in **production mode** → region `us-east1`.
4. Firestore → Rules tab → paste:

```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /debates/{debateId} {
      allow read, write: if request.auth != null
        && resource.data.userId == request.auth.uid;
    }
  }
}
```

5. Project Settings → General → copy **Project ID** and **Web API Key**.
6. Save to config:
```bash
python3 infra/setup-firebase.py
```

### Phase 6 — Wire Firebase auth into API Gateway

```bash
python3 infra/create-api-auth.py
```

Deploys `debate-firebase-authorizer` Lambda and attaches it as a TOKEN authorizer to all `/debate*` methods.

### Phase 7 — Deploy the UI

```bash
python3 infra/deploy-ui.py
```

Creates an S3 static website bucket, injects Firebase + API config into `firebase-config.js`, uploads all UI assets. Prints the public URL.

---

## Using the System

### Web UI

Open the URL printed by `deploy-ui.py`. Register an account, log in, click **+ New Debate**, ask a question. The page polls every 10 seconds and shows the result when ready (typically 1–3 minutes).

The dashboard lists all your past debates. Stuck debates (running >15 min) are highlighted, and the **Cancel & Delete** button lets you remove any debate from either the dashboard or the live debate page.

### CLI

```bash
# Register a new account
python3 auth/cli.py register --email you@example.com --password 'Pass1234'

# Log in (saves token to ~/.debate_token, valid 1h, auto-refreshes)
python3 auth/cli.py login --email you@example.com --password 'Pass1234'

# Start a debate and wait for the result inline
python3 auth/cli.py debate --query "Is AI safe in healthcare?"

# Or get the ID and poll later
python3 auth/cli.py debate --query "..." --no-wait
python3 auth/cli.py result --id D-XXXXXXXXXXXX

# Check token status
python3 auth/cli.py whoami

# Log out
python3 auth/cli.py logout
```

Password requirements: min 8 chars, at least one uppercase, lowercase, and digit.

---

## Tests

Seven failure-mode tests verify distributed-systems properties. From `milestone-4/`:

```bash
# Pre-requisites: AWS creds active, logged in via auth CLI, all 6 agent instances running

# List available tests
python3 tests/run_all.py --list

# Run all 7 (takes ~15–30 minutes)
python3 tests/run_all.py

# Run specific tests by number
python3 tests/run_all.py 1 4 5
```

### What each test verifies

| # | Test | What it does | What it proves |
|---|---|---|---|
| 1 | Agent Crash Mid-Processing | Starts a debate, terminates a research agent while it's processing | SQS visibility timeout + ASG replacement = no data loss |
| 2 | Redis Network Partition | Removes the Lambda → Redis security group rule, tries to start a debate | Orchestrator detects Redis outage and returns 503 cleanly |
| 3 | Partial Agent Failure | Terminates 1 of 2 research agents, runs a debate with reduced capacity | Single remaining worker processes all 3 angles serially; debate completes |
| 4 | Poison Message in Queue | Injects a malformed task into `research-tasks` directly | Agent's `KeyError` handler drops the bad message; DLQ catches retried failures |
| 5 | Duplicate Message Deduplication | Sends two identical messages with same `MessageDeduplicationId` | SQS FIFO dedup returns the same MessageId, queue depth increases by 1 |
| 6 | Concurrent Debates | Starts 3 debates simultaneously | All complete independently — no cross-debate state collision |
| 7 | S3 Write Failure | Removes S3 bucket policy, runs a debate, restores policy | judge_aggregator surfaces the S3 error rather than corrupting state |

### Recommended test order

Start cheap → expensive:

```bash
python3 tests/run_all.py 5    # ~10s   (SQS only, no API/agent work)
python3 tests/run_all.py 4    # ~2 min
python3 tests/run_all.py 1    # ~2 min
python3 tests/run_all.py 3    # ~5 min  (deliberately slow — single agent)
python3 tests/run_all.py 7    # ~3 min
python3 tests/run_all.py 6    # ~5 min  (3 concurrent debates)
python3 tests/run_all.py 2    # ~1 min  (orchestrator-side only)
```

### Common test pitfalls

- **HTTP 401 on every test** — your Firebase token expired (1h lifetime). Re-run `python3 auth/cli.py login ...`.
- **Test timeouts at round 2** — debates take 2–3 minutes for 3 full rounds. Don't Ctrl+C if you see `Round N/3 running...` — it's working, just running.
- **"DLQ not found" warning in test 4** — informational; the test continues without checking DLQ depth.

---

## Troubleshooting

### Debate stuck in "Round 1 of 3" forever

Check each pipeline stage in order:

```bash
# Are agents 200-OK on their LLM calls?
aws logs tail /debate/research-agent --since 10m --region us-east-1 \
  | grep -E "(HTTP/1|ERROR)" | tail -20

# Did the research aggregator fan out to critics?
aws logs tail /aws/lambda/aggregator_lambda --since 10m --region us-east-1 | tail -20

# Does this log group exist? (created on first invocation)
aws logs describe-log-groups \
  --log-group-name-prefix /aws/lambda/judge_aggregator_lambda \
  --region us-east-1
```

If a log group doesn't exist, the chain dies upstream. The most common cause is an invalid OpenAI/NVIDIA key — the 401 response is logged in the agent logs.

### ASG can't launch new instances ("template ID does not exist")

The `create-launch-template.sh` script **deletes and recreates** the template, which changes the ID. ASGs pointed at the old ID can't launch. Fix by re-pointing each ASG at the current template by name + `$Latest`:

```bash
for asg in research-agent-asg critic-agent-asg judge-agent-asg; do
  template="${asg%-asg}-template"
  aws autoscaling update-auto-scaling-group \
    --auto-scaling-group-name "$asg" \
    --launch-template "LaunchTemplateName=$template,Version=\$Latest" \
    --region us-east-1
done
```

### Lab session expired — VPC resources gone

AWS Academy wipes VPCs/subnets/SGs on session restart. ElastiCache and Lambda functions survive but lose VPC attachment.

1. Re-run `python3 infra/setup.py` — recreates VPC, updates `debate_config.json`.
2. Update the hardcoded `SECURITY_GROUP_ID` on line 10 of all three `create-launch-template.sh` scripts.
3. Re-run `python3 lambdas/create-lambdas.py` — reattaches Lambda VPC config.
4. Re-run the three `create-launch-template.sh` scripts.
5. Re-point the ASGs at the new template (see above).

### Firestore "Missing or insufficient permissions" when deleting

The Firestore rules in Phase 5 use `allow read, write`. `write` includes `create`, `update`, `delete`. If you split them out (`allow read, create, update`), `delete` is implicitly denied. Use `read, write` or explicitly add `delete`.

---

## Tech Stack

- **Compute**: AWS Lambda (Python 3.11), EC2 t3.micro (Amazon Linux 2), Auto Scaling Groups
- **Messaging**: SQS FIFO queues with content-based deduplication and DLQs
- **Coordination**: ElastiCache for Redis (TLS in transit, encryption at rest)
- **Storage**: S3 (final results), Firestore (user-facing debate metadata)
- **Auth**: Firebase Authentication (Email/Password); API Gateway TOKEN authorizer Lambda
- **LLM**: OpenAI GPT-4o (or NVIDIA NIM — agents support either via OpenAI-compatible API)
- **Frontend**: Static HTML/CSS/JS on S3 static website hosting; Firebase JS SDK
