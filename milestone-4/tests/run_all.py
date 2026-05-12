"""
run_all.py — Debatrium Distributed Systems Test Suite
─────────────────────────────────────────────────────────────
Runs all 7 failure-mode tests in sequence and prints a summary.

Usage (from milestone-4/):
  python3 tests/run_all.py              # run all tests
  python3 tests/run_all.py 2 5 6       # run only tests 2, 5, 6
  python3 tests/run_all.py --list      # show all tests without running

Prerequisites:
  - AWS credentials active (lab session not expired)
  - At least 1 research, critic, and judge agent running
  - Logged in: python3 auth/cli.py login --email ...
"""

import sys
import os
import time
import importlib

# Add milestone-4/ to path so "tests.helpers" etc. resolve correctly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TESTS = [
    (1, "Agent Crash Mid-Processing",          "tests.test_1_agent_crash",      "run"),
    (2, "Redis Network Partition",             "tests.test_2_redis_partition",   "run"),
    (3, "Partial Agent Failure (1 of 3)",      "tests.test_3_partial_agents",    "run"),
    (4, "Poison Message in Queue",             "tests.test_4_poison_message",    "run"),
    (5, "Duplicate Message Deduplication",     "tests.test_5_deduplication",     "run"),
    (6, "Concurrent Debates (Scalability)",    "tests.test_6_concurrent_debates","run"),
    (7, "S3 Write Failure",                    "tests.test_7_s3_failure",        "run"),
]

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"


def banner():
    print(f"""
{CYAN}{BOLD}
╔══════════════════════════════════════════════════════════╗
║      DEBATRIUM — Distributed Systems Test Suite         ║
║      7 Failure-Mode Tests                               ║
╚══════════════════════════════════════════════════════════╝
{RESET}""")


def list_tests():
    print(f"\n  {'#':<4} {'Test Name':<45} {'Module'}")
    print(f"  {'─'*4} {'─'*45} {'─'*30}")
    for num, name, module, _ in TESTS:
        print(f"  {num:<4} {name:<45} {module}")
    print()


def prerequisite_check():
    """Quick sanity check before running tests."""
    print(f"\n{CYAN}  Pre-flight checks{RESET}")
    print(f"  {'─'*40}")

    import os
    token_file = os.path.expanduser("~/.debate_token")
    if os.path.exists(token_file):
        print(f"  {GREEN}✓{RESET}  Auth token found ({token_file})")
    else:
        print(f"  {RED}✗{RESET}  No auth token — run: python3 auth/cli.py login ...")
        return False

    try:
        import boto3
        sts = boto3.client("sts", region_name="us-east-1")
        identity = sts.get_caller_identity()
        print(f"  {GREEN}✓{RESET}  AWS credentials valid — account {identity['Account']}")
    except Exception as e:
        print(f"  {RED}✗{RESET}  AWS credentials invalid: {e}")
        return False

    try:
        from tests.helpers import CFG, API_URL
        print(f"  {GREEN}✓{RESET}  debate_config.json loaded")
        print(f"  {GREEN}✓{RESET}  API URL: {API_URL}")
    except Exception as e:
        print(f"  {RED}✗{RESET}  Config error: {e}")
        return False

    print()
    return True


def run_tests(selected: list[int]):
    banner()

    if not prerequisite_check():
        print(f"  {RED}Aborting — fix prerequisites above.{RESET}\n")
        sys.exit(1)

    to_run  = [(n, name, mod, fn) for n, name, mod, fn in TESTS if n in selected]
    summary = []

    print(f"  Running {len(to_run)} test(s)...\n")

    for num, name, module, fn_name in to_run:
        print(f"\n{CYAN}{BOLD}  ━━━ Test {num}/{len(TESTS)}: {name} ━━━{RESET}\n")
        start = time.time()
        passed = None

        try:
            mod    = importlib.import_module(module)
            fn     = getattr(mod, fn_name)
            passed = fn()
        except KeyboardInterrupt:
            print(f"\n  {YELLOW}Test {num} interrupted by user{RESET}")
            passed = None
            summary.append((num, name, "SKIPPED", 0))
            continue
        except Exception as e:
            print(f"\n  {RED}Test {num} raised an unexpected exception:{RESET}")
            print(f"  {RED}{type(e).__name__}: {e}{RESET}")
            passed = False

        duration = int(time.time() - start)
        label    = "PASS" if passed else ("FAIL" if passed is False else "SKIP")
        summary.append((num, name, label, duration))

        # Brief pause between tests so AWS rate limits don't kick in
        if num != to_run[-1][0]:
            print(f"\n  {DIM}Pausing 10s before next test...{RESET}")
            time.sleep(10)

    # ── Print summary ─────────────────────────────────────────
    print(f"""
{CYAN}{BOLD}
╔══════════════════════════════════════════════════════════╗
║                     TEST SUMMARY                        ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")

    passed_count  = sum(1 for _, _, label, _ in summary if label == "PASS")
    failed_count  = sum(1 for _, _, label, _ in summary if label == "FAIL")
    skipped_count = sum(1 for _, _, label, _ in summary if label == "SKIPPED")

    for num, name, label, duration in summary:
        if label == "PASS":
            icon  = f"{GREEN}✓ PASS{RESET}"
        elif label == "FAIL":
            icon  = f"{RED}✗ FAIL{RESET}"
        else:
            icon  = f"{YELLOW}– SKIP{RESET}"
        print(f"  {icon}  Test {num}: {name:<45} ({duration}s)")

    print(f"""
  ─────────────────────────────────────────────────────────
  {GREEN}Passed : {passed_count}{RESET}
  {RED}Failed : {failed_count}{RESET}
  {YELLOW}Skipped: {skipped_count}{RESET}
  Total  : {len(summary)}
""")

    if failed_count == 0 and skipped_count == 0:
        print(f"  {GREEN}{BOLD}All tests passed. System is fault-tolerant.{RESET}\n")
    elif failed_count > 0:
        print(f"  {RED}{BOLD}Some tests failed — see details above for bugs to fix.{RESET}\n")
    else:
        print(f"  {YELLOW}Some tests were skipped — re-run when prerequisites are met.{RESET}\n")


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args:
        banner()
        list_tests()
        sys.exit(0)

    if args:
        try:
            selected = [int(a) for a in args]
            invalid  = [n for n in selected if n not in [t[0] for t in TESTS]]
            if invalid:
                print(f"Invalid test numbers: {invalid}. Valid: 1–{len(TESTS)}")
                sys.exit(1)
        except ValueError:
            print("Usage: python3 tests/run_all.py [test numbers...] | --list")
            sys.exit(1)
    else:
        selected = [t[0] for t in TESTS]

    run_tests(selected)
