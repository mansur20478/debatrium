"""
create-api-auth.py — Deploy Firebase token authorizer + wire into API Gateway.

Run after setup-firebase.py:
  python infra/create-api-auth.py

Steps:
  1. Package lambdas/firebase-authorizer/authorizer_lambda.py into a zip
  2. Deploy / update the debate-firebase-authorizer Lambda (no VPC, uses LabRole)
  3. Grant API Gateway permission to invoke the Lambda
  4. Auto-discover the REST API (looks for a resource at path /debate)
  5. Create a TOKEN-type Lambda authorizer
  6. Attach the authorizer to every non-OPTIONS method on every resource
  7. Deploy the API to push changes live
  8. Save the api_gateway section to debate_config.json
"""

import boto3
import io
import json
import os
import sys
import time
import zipfile
from botocore.exceptions import ClientError

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "debate_config.json")
AUTHORIZER_SRC = os.path.join(
    os.path.dirname(__file__), "..", "lambdas", "firebase-authorizer", "authorizer_lambda.py"
)

AUTHORIZER_LAMBDA_NAME = "debate-firebase-authorizer"
AUTHORIZER_NAME        = "debate-firebase-auth"

if not os.path.exists(os.path.abspath(CONFIG_FILE)):
    print("ERROR: debate_config.json not found.")
    sys.exit(1)

with open(os.path.abspath(CONFIG_FILE)) as f:
    cfg = json.load(f)

if "firebase" not in cfg:
    print("ERROR: No 'firebase' section in debate_config.json.")
    print("       Run python infra/setup-firebase.py first.")
    sys.exit(1)

REGION       = cfg["region"]
ACCOUNT_ID   = cfg["account_id"]
PROJECT_ID   = cfg["firebase"]["project_id"]
WEB_API_KEY  = cfg["firebase"]["web_api_key"]
LAB_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/LabRole"
STAGE_NAME   = cfg.get("api_gateway", {}).get("stage", "prod")

lambda_ = boto3.client("lambda",     region_name=REGION)
apigw   = boto3.client("apigateway", region_name=REGION)


def log(msg):     print(f"  -> {msg}")
def ok(msg):      print(f"  OK {msg}")
def skip(msg):    print(f"  -- {msg} (already exists)")
def warn(msg):    print(f"  !! {msg}")
def fail(msg):    print(f"  XX {msg}")
def section(msg): print(f"\n{'='*60}\n  {msg}\n{'='*60}")


# ─────────────────────────────────────────────────────────────
# STEP 1 — BUILD ZIP
# ─────────────────────────────────────────────────────────────
def build_zip() -> bytes:
    section("Step 1 — Building authorizer zip")

    src = os.path.abspath(AUTHORIZER_SRC)
    if not os.path.exists(src):
        fail(f"Source not found: {src}")
        sys.exit(1)

    log(f"Packaging: {src}")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(src, "authorizer_lambda.py")
    zip_bytes = buf.getvalue()
    ok(f"Zip built — {len(zip_bytes):,} bytes (stdlib only, no pip needed)")
    return zip_bytes


# ─────────────────────────────────────────────────────────────
# STEP 2 — DEPLOY LAMBDA
# ─────────────────────────────────────────────────────────────
def deploy_lambda(zip_bytes: bytes) -> str:
    section("Step 2 — Deploying Firebase authorizer Lambda")

    env = {
        "FIREBASE_PROJECT_ID":  PROJECT_ID,
        "FIREBASE_WEB_API_KEY": WEB_API_KEY,
    }

    try:
        existing = lambda_.get_function(FunctionName=AUTHORIZER_LAMBDA_NAME)
        arn = existing["Configuration"]["FunctionArn"]
        skip(AUTHORIZER_LAMBDA_NAME)

        log("Updating code...")
        lambda_.update_function_code(
            FunctionName=AUTHORIZER_LAMBDA_NAME,
            ZipFile=zip_bytes,
        )
        _wait_update(AUTHORIZER_LAMBDA_NAME)

        log("Updating config...")
        lambda_.update_function_configuration(
            FunctionName=AUTHORIZER_LAMBDA_NAME,
            Handler="authorizer_lambda.lambda_handler",
            Timeout=10,
            MemorySize=128,
            Environment={"Variables": env},
        )
        _wait_update(AUTHORIZER_LAMBDA_NAME)
        ok(f"Lambda updated: {arn}")
        return arn

    except lambda_.exceptions.ResourceNotFoundException:
        log(f"Creating {AUTHORIZER_LAMBDA_NAME}...")
        resp = lambda_.create_function(
            FunctionName=AUTHORIZER_LAMBDA_NAME,
            Runtime="python3.11",
            Role=LAB_ROLE_ARN,
            Handler="authorizer_lambda.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description="Validates Firebase ID tokens for API Gateway",
            Timeout=10,
            MemorySize=128,
            Environment={"Variables": env},
            PackageType="Zip",
            Tags={"Project": "Debatrium"},
        )
        arn = resp["FunctionArn"]
        _wait_active(AUTHORIZER_LAMBDA_NAME)
        ok(f"Lambda created: {arn}")
        return arn


def _wait_update(name: str, attempts: int = 40):
    for _ in range(attempts):
        resp   = lambda_.get_function_configuration(FunctionName=name)
        status = resp.get("LastUpdateStatus", "Successful")
        state  = resp.get("State", "Active")
        if status == "Successful" and state == "Active":
            return
        time.sleep(3)
    warn(f"Timed out waiting for {name} to finish updating")


def _wait_active(name: str, attempts: int = 40):
    log(f"Waiting for {name} to become Active...")
    for i in range(attempts):
        resp  = lambda_.get_function_configuration(FunctionName=name)
        state = resp.get("State", "")
        if state == "Active":
            return
        print(f"    [{i+1}/{attempts}] State: {state}")
        time.sleep(3)
    warn(f"{name} did not reach Active — check Lambda console")


# ─────────────────────────────────────────────────────────────
# STEP 3 — GRANT API GATEWAY INVOKE PERMISSION
# ─────────────────────────────────────────────────────────────
def grant_apigw_permission(lambda_arn: str):
    section("Step 3 — Granting API Gateway invoke permission")

    try:
        lambda_.add_permission(
            FunctionName=AUTHORIZER_LAMBDA_NAME,
            StatementId="apigw-invoke-authorizer",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{REGION}:{ACCOUNT_ID}:*/authorizers/*",
        )
        ok("Resource policy added for apigateway.amazonaws.com")
    except ClientError as e:
        if "already exists" in str(e).lower() or "ResourceConflictException" in str(e):
            skip("Resource policy")
        else:
            warn(f"Could not add permission: {e}")


# ─────────────────────────────────────────────────────────────
# STEP 4 — DISCOVER REST API
# ─────────────────────────────────────────────────────────────
def find_rest_api() -> dict:
    section("Step 4 — Discovering REST API")

    saved_id = cfg.get("api_gateway", {}).get("rest_api_id")
    if saved_id:
        try:
            api = apigw.get_rest_api(restApiId=saved_id)
            ok(f"Using saved API: {api['name']} ({saved_id})")
            return api
        except ClientError:
            warn(f"Saved API ID {saved_id} not found — re-discovering")

    log("Scanning REST APIs for /debate resource...")
    apis = apigw.get_rest_apis(limit=500).get("items", [])

    for api in apis:
        api_id = api["id"]
        try:
            resources = apigw.get_resources(restApiId=api_id, limit=500).get("items", [])
            if any(r.get("path") == "/debate" for r in resources):
                ok(f"Found: {api['name']} ({api_id})")
                return api
        except ClientError:
            continue

    fail("Could not find a REST API with a /debate resource.")
    print()
    print("  Add the API ID manually to debate_config.json:")
    print('  "api_gateway": { "rest_api_id": "<id>", "stage": "prod" }')
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 5 — CREATE LAMBDA AUTHORIZER
# ─────────────────────────────────────────────────────────────
def create_authorizer(api_id: str, lambda_arn: str) -> str:
    section("Step 5 — Lambda authorizer")

    existing = apigw.get_authorizers(restApiId=api_id).get("items", [])
    for auth in existing:
        if auth["name"] == AUTHORIZER_NAME:
            auth_id = auth["id"]
            skip(f"{AUTHORIZER_NAME} ({auth_id})")
            return auth_id

    log(f"Creating authorizer: {AUTHORIZER_NAME}")
    resp = apigw.create_authorizer(
        restApiId=api_id,
        name=AUTHORIZER_NAME,
        type="TOKEN",
        authorizerUri=(
            f"arn:aws:apigateway:{REGION}:lambda:path"
            f"/2015-03-31/functions/{lambda_arn}/invocations"
        ),
        identitySource="method.request.header.Authorization",
        authorizerResultTtlInSeconds=300,
    )
    auth_id = resp["id"]
    ok(f"Authorizer created: {auth_id}")
    ok(f"  Lambda : {lambda_arn}")
    ok(f"  Source : method.request.header.Authorization")
    ok(f"  TTL    : 300s")
    return auth_id


# ─────────────────────────────────────────────────────────────
# STEP 6 — ATTACH TO ALL NON-OPTIONS METHODS
# ─────────────────────────────────────────────────────────────
def attach_authorizer(api_id: str, auth_id: str):
    section("Step 6 — Attaching authorizer to all methods")

    resources = apigw.get_resources(restApiId=api_id, limit=500).get("items", [])
    updated   = 0

    for resource in resources:
        resource_id = resource["id"]
        path        = resource.get("path", "?")
        methods     = resource.get("resourceMethods", {})

        for http_method in methods:
            if http_method == "OPTIONS":
                skip(f"OPTIONS {path} — CORS preflight")
                continue

            log(f"Updating {http_method} {path}")
            try:
                apigw.update_method(
                    restApiId=api_id,
                    resourceId=resource_id,
                    httpMethod=http_method,
                    patchOperations=[
                        {"op": "replace", "path": "/authorizationType", "value": "CUSTOM"},
                        {"op": "replace", "path": "/authorizerId",      "value": auth_id},
                    ],
                )
                ok(f"  {http_method} {path} → authorizer attached")
                updated += 1
                time.sleep(0.3)
            except ClientError as e:
                warn(f"  {http_method} {path} — {e.response['Error']['Message']}")

    ok(f"Total methods updated: {updated}")


# ─────────────────────────────────────────────────────────────
# STEP 7 — DEPLOY API
# ─────────────────────────────────────────────────────────────
def deploy_api(api_id: str) -> str:
    section("Step 7 — Deploying API")

    log(f"Deploying to stage: {STAGE_NAME}")
    resp = apigw.create_deployment(
        restApiId=api_id,
        stageName=STAGE_NAME,
        description="Added Firebase Lambda authorizer",
    )
    url = f"https://{api_id}.execute-api.{REGION}.amazonaws.com/{STAGE_NAME}"
    ok(f"Deployed: {resp['id']} → stage={STAGE_NAME}")
    ok(f"API URL : {url}")
    return url


# ─────────────────────────────────────────────────────────────
# STEP 8 — SAVE CONFIG
# ─────────────────────────────────────────────────────────────
def save_config(api_id: str, api_name: str, auth_id: str, url: str):
    section("Step 8 — Saving config")

    cfg["api_gateway"] = {
        "rest_api_id": api_id,
        "api_name":    api_name,
        "stage":       STAGE_NAME,
        "url":         url,
    }
    cfg["firebase"]["authorizer_id"] = auth_id

    with open(os.path.abspath(CONFIG_FILE), "w") as f:
        json.dump(cfg, f, indent=2)
    ok("debate_config.json updated")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  DEBATE SYSTEM — API Gateway Auth Wiring (Firebase)")
    print("=" * 60)
    print(f"\n  Region   : {REGION}")
    print(f"  Firebase : {PROJECT_ID}")
    print(f"  Stage    : {STAGE_NAME}")
    print()

    zip_bytes  = build_zip()
    lambda_arn = deploy_lambda(zip_bytes)
    grant_apigw_permission(lambda_arn)
    api        = find_rest_api()
    auth_id    = create_authorizer(api["id"], lambda_arn)
    attach_authorizer(api["id"], auth_id)
    url        = deploy_api(api["id"])
    save_config(api["id"], api["name"], auth_id, url)

    print("\n" + "=" * 60)
    print("  DONE — API is now protected by Firebase auth")
    print("=" * 60)
    print(f"\n  API URL : {url}")
    print()
    print("  All routes require:  Authorization: Bearer <Firebase ID token>")
    print("  OPTIONS routes are open (CORS preflight)")
    print()
    print("  Next steps:")
    print("    python auth/cli.py register --email you@example.com --password 'Pass1234'")
    print("    python auth/cli.py login    --email you@example.com --password 'Pass1234'")
    print('    python auth/cli.py debate   --query "Is AI safe in healthcare?"')
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
