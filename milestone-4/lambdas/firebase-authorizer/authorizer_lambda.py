"""
Firebase ID Token authorizer for API Gateway (TOKEN type).

Validates Firebase ID tokens by calling Firebase's accounts:lookup endpoint.
No external packages required — stdlib only.

Environment variables:
  FIREBASE_PROJECT_ID  — Firebase project ID
  FIREBASE_WEB_API_KEY — Firebase Web API key
"""

import json
import os
import urllib.request
import urllib.error

FIREBASE_PROJECT_ID  = os.environ["FIREBASE_PROJECT_ID"]
FIREBASE_WEB_API_KEY = os.environ["FIREBASE_WEB_API_KEY"]

LOOKUP_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:lookup"
    f"?key={FIREBASE_WEB_API_KEY}"
)


def verify_token(id_token: str) -> dict:
    payload = json.dumps({"idToken": id_token}).encode()
    req = urllib.request.Request(
        LOOKUP_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err = json.loads(e.read().decode())
        raise ValueError(err.get("error", {}).get("message", "Token invalid"))

    users = result.get("users", [])
    if not users:
        raise ValueError("Token invalid: no user found")
    return users[0]


def wildcard_arn(method_arn: str) -> str:
    # arn:aws:execute-api:{region}:{account}:{api-id}/{stage}/{method}/{resource}
    # → arn:aws:execute-api:{region}:{account}:{api-id}/{stage}/*/*
    parts         = method_arn.split(":")
    resource_path = parts[5].split("/")
    base          = ":".join(parts[:5]) + ":" + "/".join(resource_path[:2])
    return base + "/*/*"


def make_policy(principal_id: str, effect: str, resource: str, context: dict = None) -> dict:
    policy = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action":   "execute-api:Invoke",
                "Effect":   effect,
                "Resource": resource,
            }],
        },
    }
    if context:
        policy["context"] = {k: str(v) for k, v in context.items()}
    return policy


def lambda_handler(event, context):
    token      = event.get("authorizationToken", "")
    method_arn = event.get("methodArn", "")

    if token.lower().startswith("bearer "):
        token = token[7:]

    if not token:
        raise Exception("Unauthorized")

    try:
        user  = verify_token(token)
        uid   = user.get("localId", "unknown")
        email = user.get("email", "")
        print(f"Authorized: uid={uid} email={email}")
        return make_policy(uid, "Allow", wildcard_arn(method_arn), {"uid": uid, "email": email})
    except Exception as e:
        print(f"Authorization denied: {e}")
        raise Exception("Unauthorized")
