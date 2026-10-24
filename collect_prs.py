#!/usr/bin/env python3
"""
collect_prs.py - data collector for the "PR triage" dashboard.

Emits one row per OPEN pull request in rpnunez/wp-ai-scheduler, compared against
`main`, as a bare JSON array written to ./out/pr_data.json (next to this script).
Read-only: it never pushes or mutates anything on GitHub.

Requires: Python 3, `gh` and `git` (git >= 2.38 for `git merge-tree --write-tree`).
Standard library only.

AUTH
  The token is read from env var PRDASH_GH_TOKEN (fallback: GH_TOKEN). If neither
  is set in the environment, a `.env` file next to this script is read for the same
  names (see .env.example); real environment variables always win. It is passed
  to `gh`/`git` only through the subprocess environment (GH_TOKEN) and is never
  printed, logged or written to disk. `gh auth setup-git` is run once so git
  clone/fetch use it too. NOTE: that command edits your global git config
  (credential helper for github.com); the helper reads the token from GH_TOKEN
  at call time, so no secret is stored.

PAT SCOPES - READ-ONLY, nothing more
  Option 1: Fine-grained PAT (preferred)
    Resource owner      = rpnunez
    Repository access   = Only select repositories -> wp-ai-scheduler
    Repository permissions, all Read:
      * Metadata: Read         (required, auto-selected)
      * Contents: Read         (clone + fetch branches and PR head refs)
      * Pull requests: Read    (list open PRs and their metadata)
      * Commit statuses: Read  (combined status, used only as a CI fallback)
      * Checks: Read           (check-runs for CI status) - only if your token UI
                               lists it; fine-grained PATs may not. The repo is
                               public, so check-runs are usually readable anyway.
                               If every PR logs "CI lookup failed", use Option 2.
    Do NOT grant any Write permission and no account permissions.

  Option 2: Classic PAT (alternative)
    The repo is public, so the `public_repo` scope is sufficient for all of the
    reads above. Use `repo` only if the repo ever becomes private.

DATA SOURCES
  PR metadata and CI: GitHub API via `gh api`.
  Sizes, changed files, ahead/behind, conflicts: local full clone + local 3-way
  merge (`git merge-tree`). GitHub's own `mergeable` flag is deliberately NOT used.

OUTPUT
  stdout is kept clean; warnings and the final summary go to stderr.
"""

import atexit
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

REPO = "rpnunez/wp-ai-scheduler"
BASE = "main"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
OUT_FILE = os.path.join(OUT_DIR, "pr_data.json")
ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
TOKEN_VARS = ("PRDASH_GH_TOKEN", "GH_TOKEN")

FAILING_CONCLUSIONS = {
    "failure", "timed_out", "cancelled", "action_required", "startup_failure", "stale",
}

_TOKEN = ""


def log(msg):
    text = str(msg)
    if _TOKEN:
        text = text.replace(_TOKEN, "***")
    print(text, file=sys.stderr, flush=True)


def die(msg, code=1):
    log("ERROR: " + str(msg))
    sys.exit(code)


def read_env_file(path):
    """Minimal .env parser (KEY=VALUE, '#' comments, optional quotes / 'export ')."""
    values = {}
    try:
        with open(path, encoding="utf-8-sig") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                key, _, val = line.partition("=")
                val = val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                    val = val[1:-1]
                values[key.strip()] = val
    except FileNotFoundError:
        pass
    except OSError as e:
        log("WARNING: could not read %s: %s" % (path, e))
    return values


def find_token():
    """Environment first (PRDASH_GH_TOKEN, then GH_TOKEN); .env file only as a fallback."""
    for name in TOKEN_VARS:
        if os.environ.get(name):
            return os.environ[name]
    file_vals = read_env_file(ENV_FILE)
    for name in TOKEN_VARS:
        if file_vals.get(name):
            return file_vals[name]
    return ""


def run(cmd, env, cwd=None):
    """Run a command, capture text output. Never raises on non-zero exit."""
    return subprocess.run(
        cmd, env=env, cwd=cwd, capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )


def git(tmp, env, *args):
    return run(["git", "-c", "core.quotePath=false", "-C", tmp, *args], env)


def git_ok(tmp, env, *args):
    r = git(tmp, env, *args)
    if r.returncode != 0:
        raise RuntimeError("git %s failed (exit %d): %s" % (args[0], r.returncode, r.stderr.strip()))
    return r.stdout


def parse_json_stream(text):
    """`gh api --paginate` prints one JSON document per page, back to back."""
    dec = json.JSONDecoder()
    pos, docs, n = 0, [], len(text)
    while pos < n:
        while pos < n and text[pos].isspace():
            pos += 1
        if pos >= n:
            break
        doc, pos = dec.raw_decode(text, pos)
        docs.append(doc)
    return docs


def gh_api(env, endpoint, paginate=False):
    cmd = ["gh", "api"] + (["--paginate"] if paginate else []) + [endpoint]
    r = run(cmd, env)
    if r.returncode != 0:
        raise RuntimeError("gh api %s failed (exit %d): %s" % (endpoint.split("?")[0], r.returncode, r.stderr.strip()))
    docs = parse_json_stream(r.stdout)
    return docs if paginate else (docs[0] if docs else None)


def to_utc_z(value):
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ci_status(env, sha):
    pages = gh_api(env, "repos/%s/commits/%s/check-runs?per_page=100" % (REPO, sha), paginate=True)
    runs = [cr for page in pages for cr in page.get("check_runs", [])]
    if runs:
        if any(cr.get("status") != "completed" for cr in runs):
            return "pending"
        concl = {cr.get("conclusion") for cr in runs}
        if concl & FAILING_CONCLUSIONS:
            return "failing"
        if "success" in concl:
            return "passing"
        return "none"
    # Fallback: legacy commit statuses, only when there are no check runs at all.
    st = gh_api(env, "repos/%s/commits/%s/status" % (REPO, sha))
    if not st or not st.get("total_count"):
        return "none"
    return {"success": "passing", "failure": "failing", "error": "failing",
            "pending": "pending"}.get(st.get("state"), "none")


def git_metrics(tmp, env, pr_ref):
    ahead = int(git_ok(tmp, env, "rev-list", "--count", "%s..%s" % (BASE, pr_ref)).strip())
    behind = int(git_ok(tmp, env, "rev-list", "--count", "%s..%s" % (pr_ref, BASE)).strip())

    names = git_ok(tmp, env, "diff", "--name-only", "-z", "%s...%s" % (BASE, pr_ref))
    files = [f for f in names.split("\0") if f]

    adds = dels = 0
    for line in git_ok(tmp, env, "diff", "--numstat", "%s...%s" % (BASE, pr_ref)).splitlines():
        cols = line.split("\t")
        if len(cols) < 2:
            continue
        adds += int(cols[0]) if cols[0].isdigit() else 0  # "-" = binary
        dels += int(cols[1]) if cols[1].isdigit() else 0

    has_conflicts, conflict_files = False, []
    m = git(tmp, env, "merge-tree", "--write-tree", "--name-only", BASE, pr_ref)
    if m.returncode == 0:
        pass
    elif m.returncode == 1:
        has_conflicts = True
        for line in m.stdout.split("\n")[1:]:  # line 0 is the tree OID
            if line == "":
                break
            if line not in conflict_files:
                conflict_files.append(line)
    else:
        log("WARNING: merge-tree exit %d for %s; treating conflicts as unknown: %s"
            % (m.returncode, pr_ref, m.stderr.strip()))

    last = git_ok(tmp, env, "show", "-s", "--format=%cI", pr_ref).strip()
    return {
        "additions": adds, "deletions": dels, "changed_files": len(files),
        "ahead_by": ahead, "behind_by": behind,
        "has_conflicts": has_conflicts, "conflict_files": conflict_files,
        "files": files, "last_commit_at": to_utc_z(last),
    }


def rmtree_force(path):
    def onerror(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)  # git objects are read-only on Windows
            func(p)
        except OSError:
            pass
    shutil.rmtree(path, onerror=onerror)


def main():
    global _TOKEN
    started = datetime.now(timezone.utc)
    snapshot_at = started.strftime("%Y-%m-%dT%H:%M:%SZ")

    _TOKEN = find_token()
    if not _TOKEN:
        die("no token found: set PRDASH_GH_TOKEN (or GH_TOKEN) in the environment, "
            "or in %s, to a read-only GitHub PAT." % ENV_FILE)
    for tool in ("gh", "git"):
        if shutil.which(tool) is None:
            die("`%s` not found on PATH." % tool)

    env = dict(os.environ)
    env["GH_TOKEN"] = _TOKEN
    env.pop("GITHUB_TOKEN", None)  # gh prefers GH_TOKEN, but avoid ambiguity
    env["GH_PROMPT_DISABLED"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"

    if run(["gh", "auth", "status"], env).returncode != 0:
        die("`gh auth status` failed: the token is invalid or expired.")
    r = run(["gh", "auth", "setup-git"], env)
    if r.returncode != 0:
        die("`gh auth setup-git` failed: " + r.stderr.strip())

    try:
        pulls = [p for page in gh_api(env, "repos/%s/pulls?state=open&per_page=100" % REPO, paginate=True)
                 for p in page]
    except Exception as e:
        die("could not list open PRs: %s" % e)
    log("Found %d open PRs." % len(pulls))

    tmp = tempfile.mkdtemp(prefix="prdash_")
    atexit.register(rmtree_force, tmp)
    try:
        r = run(["gh", "repo", "clone", REPO, tmp], env)
        if r.returncode != 0:
            die("clone failed: " + r.stderr.strip())
        # Full history is required (no --depth) for merge-base detection.
        if git(tmp, env, "rev-parse", "--verify", "--quiet", BASE).returncode != 0:
            die("branch '%s' not found in clone." % BASE)
        f = git(tmp, env, "fetch", "origin", "+refs/pull/*/head:refs/remotes/pr/*")
        if f.returncode != 0:
            log("WARNING: fetching PR head refs reported errors (will fall back to head.sha): "
                + f.stderr.strip())

        rows = []
        for pr in pulls:
            num = pr.get("number")
            try:
                sha = pr["head"]["sha"]
                ref = "refs/remotes/pr/%d" % num
                if git(tmp, env, "rev-parse", "--verify", "--quiet", ref + "^{commit}").returncode != 0:
                    ref = sha
                gm = git_metrics(tmp, env, ref)
            except Exception as e:
                log("WARNING: PR #%s skipped (git data unavailable): %s" % (num, e))
                continue
            try:
                ci = ci_status(env, sha)
            except Exception as e:
                log("WARNING: PR #%s CI lookup failed, using 'none': %s" % (num, e))
                ci = "none"
            rows.append({
                "number": num,
                "title": pr.get("title") or "",
                "author": (pr.get("user") or {}).get("login") or "",
                "branch": pr["head"].get("ref") or "",
                "draft": bool(pr.get("draft")),
                "additions": gm["additions"],
                "deletions": gm["deletions"],
                "changed_files": gm["changed_files"],
                "ahead_by": gm["ahead_by"],
                "behind_by": gm["behind_by"],
                "has_conflicts": gm["has_conflicts"],
                "conflict_files": gm["conflict_files"],
                "files": gm["files"],
                "ci": ci,
                "created_at": to_utc_z(pr["created_at"]),
                "last_commit_at": gm["last_commit_at"],
                "snapshot_at": snapshot_at,
            })
    finally:
        rmtree_force(tmp)

    rows.sort(key=lambda x: x["number"], reverse=True)

    os.makedirs(OUT_DIR, exist_ok=True)
    part = OUT_FILE + ".tmp"
    with open(part, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(rows, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    os.replace(part, OUT_FILE)

    ci_counts = {k: 0 for k in ("failing", "pending", "passing", "none")}
    for row in rows:
        ci_counts[row["ci"]] += 1
    log("Wrote %s" % OUT_FILE)
    log("Summary: %d PRs (of %d open), %d with conflicts; CI: %s" % (
        len(rows), len(pulls), sum(r["has_conflicts"] for r in rows),
        ", ".join("%s=%d" % kv for kv in ci_counts.items())))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        die("interrupted.", 130)
    except SystemExit:
        raise
    except Exception as exc:
        die("unexpected failure: %s" % exc)
