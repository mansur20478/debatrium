"""
auth/cli.py — Debate System CLI

Uses Firebase Authentication (email/password) via REST API.
No AWS SDK required for auth — pure urllib.

Run from the milestone-4/ directory:
  python auth/cli.py register --email you@example.com --password 'Pass1234'
  python auth/cli.py login    --email you@example.com --password 'Pass1234'
  python auth/cli.py whoami
  python auth/cli.py logout
  python auth/cli.py debate   --query "Is AI safe in healthcare?"
  python auth/cli.py result   --id D-ABC123456789

Token is saved to ~/.debate_token after login and auto-refreshed on use.
"""

import argparse
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.request

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "debate_config.json")
TOKEN_FILE  = os.path.expanduser("~/.debate_token")

_FB_BASE    = "https://identitytoolkit.googleapis.com/v1/accounts"
_FB_REFRESH = "https://securetoken.googleapis.com/v1/token"


def load_config() -> dict:
    path = os.path.abspath(CONFIG_FILE)
    if not os.path.exists(path):
        die(f"debate_config.json not found at {path}")
    with open(path) as f:
        return json.load(f)


def require_section(cfg: dict, section: str):
    scripts = {"firebase": "setup-firebase.py", "api_gateway": "create-api-auth.py"}
    if section not in cfg:
        die(
            f"No '{section}' section in debate_config.json.\n"
            f"  Run: python infra/{scripts.get(section, section + '.py')}"
        )


def die(msg: str):
    print(f"\n  ERROR: {msg}\n", file=sys.stderr)
    sys.exit(1)


def ok(msg):   print(f"  OK  {msg}")
def log(msg):  print(f"  ->  {msg}")
def info(msg): print(f"      {msg}")


# ─────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────

def _post(url: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err = json.loads(e.read().decode())
        raise RuntimeError(err.get("error", {}).get("message", "Unknown error"))


def _api(url: str, method: str = "GET", body: dict = None, token: str = None) -> dict:
    data    = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        try:
            err = json.loads(body_text)
        except Exception:
            err = {"error": body_text}
        if e.code == 401:
            die("Unauthorized — token expired or invalid. Run: python auth/cli.py login ...")
        die(f"HTTP {e.code}: {err.get('error', err)}")
    except urllib.error.URLError as e:
        die(f"Network error: {e.reason}")


# ─────────────────────────────────────────────────────────────
# TOKEN STORAGE
# ─────────────────────────────────────────────────────────────

def save_token(data: dict):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)
    os.chmod(TOKEN_FILE, 0o600)


def load_token() -> dict:
    if not os.path.exists(TOKEN_FILE):
        die(
            "Not logged in. Run:\n"
            "    python auth/cli.py login --email you@example.com --password 'Pass1234'"
        )
    with open(TOKEN_FILE) as f:
        return json.load(f)


def refresh_if_needed(cfg: dict, token_data: dict) -> dict:
    elapsed   = time.time() - token_data["logged_in_at"]
    remaining = token_data["expires_in"] - elapsed
    if remaining > 300:
        return token_data

    log("Token near expiry — refreshing...")
    api_key = cfg["firebase"]["web_api_key"]
    try:
        resp = _post(
            f"{_FB_REFRESH}?key={api_key}",
            {"grant_type": "refresh_token", "refresh_token": token_data["refresh_token"]},
        )
        token_data.update({
            "id_token":      resp["id_token"],
            "refresh_token": resp["refresh_token"],
            "expires_in":    int(resp.get("expires_in", 3600)),
            "logged_in_at":  time.time(),
        })
        save_token(token_data)
        ok("Token refreshed")
    except RuntimeError as e:
        die(f"Token refresh failed: {e}\nRun: python auth/cli.py login ...")
    return token_data


# ─────────────────────────────────────────────────────────────
# COMMANDS
# ─────────────────────────────────────────────────────────────

def cmd_register(args):
    cfg     = load_config()
    require_section(cfg, "firebase")
    api_key = cfg["firebase"]["web_api_key"]

    email    = args.email    or input("Email: ").strip()
    password = args.password or getpass.getpass("Password (min 8 chars, upper+lower+digit): ")

    print(f"\n  Registering {email} ...")
    try:
        resp = _post(
            f"{_FB_BASE}:signUp?key={api_key}",
            {"email": email, "password": password, "returnSecureToken": False},
        )
    except RuntimeError as e:
        msg = str(e)
        if "EMAIL_EXISTS"    in msg: die(f"{email} already has an account — use 'login' instead.")
        if "WEAK_PASSWORD"   in msg: die("Password too weak — min 8 chars with upper, lower, digit.")
        if "INVALID_EMAIL"   in msg: die("Invalid email address.")
        die(f"Registration failed: {msg}")

    ok(f"Account created for {email}")
    print()
    print(f"  Log in now:")
    print(f"    python auth/cli.py login --email {email}")


def cmd_login(args):
    cfg     = load_config()
    require_section(cfg, "firebase")
    api_key = cfg["firebase"]["web_api_key"]

    email    = args.email    or input("Email: ").strip()
    password = args.password or getpass.getpass("Password: ")

    print(f"\n  Logging in as {email} ...")
    try:
        resp = _post(
            f"{_FB_BASE}:signInWithPassword?key={api_key}",
            {"email": email, "password": password, "returnSecureToken": True},
        )
    except RuntimeError as e:
        msg = str(e)
        if any(k in msg for k in ("INVALID_PASSWORD", "INVALID_LOGIN_CREDENTIALS", "EMAIL_NOT_FOUND")):
            die("Invalid email or password.")
        if "USER_DISABLED" in msg:
            die("This account has been disabled.")
        die(f"Login failed: {msg}")

    expires_in = int(resp.get("expiresIn", 3600))
    save_token({
        "email":         email,
        "uid":           resp.get("localId", ""),
        "id_token":      resp["idToken"],
        "refresh_token": resp["refreshToken"],
        "expires_in":    expires_in,
        "logged_in_at":  time.time(),
    })

    ok(f"Logged in as {email}")
    ok(f"Token saved to {TOKEN_FILE}")
    info(f"Token valid for {expires_in // 3600}h (auto-refreshes on use)")
    print()
    print("  Start a debate:")
    print('    python auth/cli.py debate --query "Is AI safe in healthcare?"')


def cmd_whoami(args):
    t       = load_token()
    elapsed = time.time() - t["logged_in_at"]
    remaining = t["expires_in"] - elapsed

    print(f"\n  Logged in as : {t['email']}")
    print(f"  Firebase UID : {t.get('uid', '?')}")
    if remaining > 0:
        print(f"  Token expires: in {int(remaining // 60)} minute(s)")
    else:
        print("  Token status : expired — will auto-refresh on next command")
    print()


def cmd_logout(args):
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
        ok(f"Logged out — {TOKEN_FILE} removed")
    else:
        print("  Not logged in.")


def cmd_debate(args):
    cfg = load_config()
    require_section(cfg, "firebase")
    require_section(cfg, "api_gateway")

    query      = args.query or input("Debate query: ").strip()
    api_url    = cfg["api_gateway"]["url"].rstrip("/")
    token_data = refresh_if_needed(cfg, load_token())
    id_token   = token_data["id_token"]

    print(f"\n  Starting debate...")
    log(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")

    resp      = _api(f"{api_url}/debate", method="POST", body={"query": query}, token=id_token)
    debate_id = resp.get("debate_id")
    if not debate_id:
        die(f"Unexpected response: {resp}")

    ok(f"Debate started — ID: {debate_id}")
    info(f"Round 1 of {resp.get('max_rounds', 3)} underway")
    print()

    if args.no_wait:
        print(f"  Poll later:")
        print(f"    python auth/cli.py result --id {debate_id}")
        return

    _poll(api_url, id_token, debate_id)


def cmd_result(args):
    cfg = load_config()
    require_section(cfg, "api_gateway")

    api_url    = cfg["api_gateway"]["url"].rstrip("/")
    token_data = refresh_if_needed(cfg, load_token())
    _poll(api_url, token_data["id_token"], args.id)


def _poll(api_url: str, id_token: str, debate_id: str):
    url      = f"{api_url}/debate/{debate_id}/result"
    interval = 10
    max_wait = 600
    started  = time.time()
    last_round = None

    print(f"  Polling for result (debate_id={debate_id}) — Ctrl+C to stop\n")

    while True:
        resp   = _api(url, token=id_token)
        status = resp.get("status")

        if status == "not_found":
            die(f"Debate {debate_id} not found.")

        if status == "complete":
            _print_result(debate_id, resp.get("result", {}))
            return

        current_round = resp.get("current_round", "?")
        if current_round != last_round:
            ok(f"Round {current_round}/{resp.get('max_rounds', '?')} in progress...")
            last_round = current_round

        if time.time() - started > max_wait:
            print(f"\n  Timed out after {max_wait // 60} minutes. Check later:")
            print(f"    python auth/cli.py result --id {debate_id}")
            return

        time.sleep(interval)


def _print_result(debate_id: str, result: dict):
    bar = "=" * 60
    print(bar)
    print(f"  DEBATE RESULT — {debate_id}")
    print(bar)

    if not result:
        print("  (no result data available)")
        print(bar)
        return

    verdict = result.get("verdict",           result.get("final_verdict", ""))
    score   = result.get("score",             result.get("confidence_score", ""))
    summary = result.get("summary",           result.get("final_summary", ""))
    rounds  = result.get("rounds_completed",  "?")

    if verdict: print(f"\n  Verdict : {verdict}")
    if score:   print(f"  Score   : {score}")
    if rounds:  print(f"  Rounds  : {rounds}")
    if summary:
        print(f"\n  Summary :\n")
        for line in str(summary).splitlines():
            print(f"    {line}")

    shown  = {"verdict", "final_verdict", "score", "confidence_score",
               "summary", "final_summary", "rounds_completed"}
    extras = {k: v for k, v in result.items() if k not in shown}
    if extras:
        print(f"\n  Details :\n    {json.dumps(extras, indent=4)}")

    print()
    print(bar + "\n")


# ─────────────────────────────────────────────────────────────
# PARSER
# ─────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python auth/cli.py",
        description="Debate System CLI — Firebase-authenticated access",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    reg = sub.add_parser("register", help="Create a new account")
    reg.add_argument("--email",    help="Email address")
    reg.add_argument("--password", help="Password (min 8 chars)")

    log_ = sub.add_parser("login", help="Log in and save token")
    log_.add_argument("--email",    help="Email address")
    log_.add_argument("--password", help="Password")

    sub.add_parser("whoami", help="Show current user and token status")
    sub.add_parser("logout", help="Remove saved token")

    deb = sub.add_parser("debate", help="Start a new debate")
    deb.add_argument("--query",    help="The debate question")
    deb.add_argument("--no-wait",  action="store_true",
                     help="Return debate ID immediately without polling")

    res = sub.add_parser("result", help="Poll for a debate result")
    res.add_argument("--id", required=True, help="Debate ID returned by 'debate'")

    return parser


def main():
    args = build_parser().parse_args()
    {
        "register": cmd_register,
        "login":    cmd_login,
        "whoami":   cmd_whoami,
        "logout":   cmd_logout,
        "debate":   cmd_debate,
        "result":   cmd_result,
    }[args.command](args)


if __name__ == "__main__":
    main()
