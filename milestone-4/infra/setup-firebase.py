"""
setup-firebase.py — Configure Firebase Authentication for the debate system.

Run this before create-api-auth.py:
  python infra/setup-firebase.py

Before running, set up Firebase:
  1. Go to https://console.firebase.google.com
  2. Create a project (or open an existing one)
  3. Authentication → Sign-in method → Email/Password → Enable
  4. Project Settings (gear icon) → General tab:
       - Copy the Project ID
       - Under "Your apps", add a Web app if none exists
       - Copy the Web API key

After running this, run:
  python infra/create-api-auth.py
"""

import json
import os
import sys
import urllib.request
import urllib.error

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "debate_config.json")


def log(msg):     print(f"  -> {msg}")
def ok(msg):      print(f"  OK {msg}")
def fail(msg):    print(f"  XX {msg}")
def section(msg): print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def test_api_key(api_key: str) -> bool:
    """
    Probe the Firebase key by attempting sign-in with a dummy credential.
    A valid key returns a Firebase auth error (e.g. INVALID_LOGIN_CREDENTIALS).
    An invalid key returns API_KEY_INVALID.
    """
    url  = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"
    data = json.dumps({
        "email": "probe@debatrium-invalid.example.com",
        "password": "invalid",
        "returnSecureToken": False,
    }).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        return True  # Unexpected success — key is valid
    except urllib.error.HTTPError as e:
        msg = json.loads(e.read().decode()).get("error", {}).get("message", "")
        if "API_KEY_INVALID" in msg or "API key not valid" in msg:
            return False
        return True  # Any other Firebase error means the key itself is valid
    except Exception:
        return False


def main():
    print("\n" + "=" * 60)
    print("  DEBATE SYSTEM — Firebase Auth Setup")
    print("=" * 60)

    print("""
  Before continuing, complete these steps in Firebase console:

  1. https://console.firebase.google.com → create or open a project
  2. Authentication → Sign-in method → Email/Password → Enable
  3. Project Settings (gear) → General:
       - Note the Project ID
       - Under "Your apps": add a Web app, then copy the Web API key
  """)

    path = os.path.abspath(CONFIG_FILE)
    if not os.path.exists(path):
        fail(f"debate_config.json not found at {path}")
        sys.exit(1)

    with open(path) as f:
        cfg = json.load(f)

    if "firebase" in cfg:
        ex = cfg["firebase"]
        print(f"  Firebase already configured:")
        print(f"    Project ID  : {ex['project_id']}")
        print(f"    Web API Key : {ex['web_api_key'][:8]}...")
        answer = input("\n  Re-configure? [y/N]: ").strip().lower()
        if answer != "y":
            print("\n  Keeping existing config.")
            print("  Next: python infra/create-api-auth.py\n")
            return

    section("Step 1 — Firebase credentials")

    project_id = input("  Firebase Project ID : ").strip()
    if not project_id:
        fail("Project ID cannot be empty.")
        sys.exit(1)

    api_key = input("  Firebase Web API Key: ").strip()
    if not api_key:
        fail("Web API Key cannot be empty.")
        sys.exit(1)

    section("Step 2 — Validating credentials")

    log("Testing Web API key against Firebase...")
    if test_api_key(api_key):
        ok("API key is valid")
    else:
        fail("API key is invalid. Check Project Settings → General → Web API key.")
        sys.exit(1)

    section("Step 3 — Saving config")

    cfg["firebase"] = {
        "project_id":  project_id,
        "web_api_key": api_key,
    }
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)

    ok("debate_config.json updated with firebase section")

    print("\n" + "=" * 60)
    print("  DONE")
    print("=" * 60)
    print(f"\n  Project ID  : {project_id}")
    print(f"  Web API Key : {api_key[:8]}...")
    print()
    print("  Next: python infra/create-api-auth.py")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
