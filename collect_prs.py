#!/usr/bin/env python3
"""
collect_prs.py - data collector for the "PR triage" dashboard.

Emits one row per OPEN pull request in rpnunez/wp-ai-scheduler, compared against
`main`, as a bare JSON array written to ./out/pr_data.json (next to this script).
Read-only: it never pushes or mutates anything on GitHub.

Requires: Python 3 and git (uses `merge-tree --write-tree` when available, legacy
`git merge-tree` fallback on older git). `gh` is NOT required. Standard library only.

AUTH
  The token is read from env var PRDASH_GH_TOKEN (fallback: GH_TOKEN). If neither
  is set in the environment, a `.env` file next to this script is read for the same
  names (see .env.example); real environment variables always win. It is used ONLY
  as the Authorization header of direct GitHub REST API calls (urllib). It is never
  passed to git, put on a command line or in a URL, printed, logged or written to
  disk. git clone/fetch are anonymous because the repo is public.

PAT SCOPES - READ-ONLY, nothing more
  Option 1: Fine-grained PAT (preferred)
    Resource owner      = rpnunez
    Repository access   = Only select repositories -> wp-ai-scheduler
    Repository permissions, all Read:
      * Metadata: Read         (required, auto-selected)
      * Pull requests: Read    (list open PRs and their metadata)
      * Contents: Read         (NOT needed while the repo is public: git is
                               anonymous. Only needed if the repo becomes private
                               and git auth is added.)
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
  PR metadata and CI: GitHub REST API, called directly with urllib (sequential
  requests, bounded retry/backoff, rate-limit headers honored).
  Sizes, changed files, ahead/behind, conflicts: local full clone + local 3-way
  merge (`git merge-tree`). GitHub's own `mergeable` flag is deliberately NOT used.

OUTPUT
  stdout is kept clean; warnings and the final summary go to stderr.
"""

import atexit
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import http.client
import urllib.error
import urllib.request
from datetime import datetime, timezone

REPO = "rpnunez/wp-ai-scheduler"
BASE = "main"
API = "https://api.github.com"
USER_AGENT = "prdash-collect"
HTTP_TIMEOUT = 30   # seconds per request
MAX_ATTEMPTS = 4    # per request, transient failures only (waits 1s, 2s, 4s by default)
MAX_WAIT = 90       # never sleep longer than this in one go; fail instead of hanging
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


class ApiError(RuntimeError):
    """Non-transient API failure (404, permissions, ...): callers may degrade per PR."""


class ApiAbort(RuntimeError):
    """Rate limit or outage we cannot ride out: the whole run must stop."""


class _SameHostRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not newurl.startswith(API + "/"):
            return None  # never forward the Authorization header to another host
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_SameHostRedirect)
_quota = {"not_before": 0.0}  # epoch time before which no request may be sent


def _wait_for_quota(label):
    wait = _quota["not_before"] - time.time()
    if wait <= 0:
        return
    if wait > MAX_WAIT:
        raise ApiAbort("GET %s: rate limit exhausted, resets in %ds (MAX_WAIT is %ds)"
                       % (label, int(wait), MAX_WAIT))
    log("Rate limit exhausted; waiting %ds for reset." % (int(wait) + 1))
    time.sleep(wait)


def _note_quota(headers):
    reset = headers.get("X-RateLimit-Reset", "")
    if headers.get("X-RateLimit-Remaining") == "0" and reset.isdigit():
        _quota["not_before"] = int(reset) + 1


def _error_message(err):
    try:
        return str(json.loads(err.read().decode("utf-8")).get("message") or "")
    except (ValueError, AttributeError, OSError):
        return ""


def _is_rate_limited(headers, message):
    return bool(headers.get("Retry-After")
                or headers.get("X-RateLimit-Remaining") == "0"
                or "rate limit" in message.lower())


def _retry_wait(headers, attempt):
    """Seconds to wait before retrying: the server's own signal, else 1s, 2s, 4s..."""
    retry_after = headers.get("Retry-After", "")
    if retry_after.isdigit():
        return int(retry_after)
    reset = headers.get("X-RateLimit-Reset", "")
    if headers.get("X-RateLimit-Remaining") == "0" and reset.isdigit():
        return max(int(reset) - int(time.time()), 0) + 1
    return 2 ** (attempt - 1)


def _next_link(link_header):
    """URL of rel="next" in a Link header, or None on the last page."""
    for part in (link_header or "").split(","):
        url_part, _, rel = part.partition(";")
        if 'rel="next"' in rel:
            url = url_part.strip().strip("<>")
            if not url.startswith(API + "/"):
                raise ApiError("refusing to follow pagination link to another host")
            return url
    return None


def _get_json(url):
    """GET one URL -> (parsed JSON, next-page URL or None). Sequential; bounded retries
    for transient failures only (429, rate-limit 403, 5xx, network errors)."""
    label = url.split("?")[0][len(API):]  # path only, for messages
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + _TOKEN,
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _wait_for_quota(label)
        try:
            with _OPENER.open(urllib.request.Request(url, headers=headers),
                              timeout=HTTP_TIMEOUT) as resp:
                hdrs, body = resp.headers, resp.read()
        except urllib.error.HTTPError as e:
            hdrs, message = e.headers, _error_message(e)
            if e.code == 429 or (e.code == 403 and _is_rate_limited(hdrs, message)):
                problem = "rate limited (HTTP %d)" % e.code
            elif e.code >= 500:
                problem = "HTTP %d" % e.code
            else:
                raise ApiError("GET %s failed: HTTP %d%s"
                               % (label, e.code, ": " + message if message else "")) from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
            hdrs, problem = {}, "network error (%s)" % getattr(e, "reason", e)
        else:
            _note_quota(hdrs)
            try:
                return json.loads(body.decode("utf-8")), _next_link(hdrs.get("Link"))
            except ValueError:
                raise ApiError("GET %s returned invalid JSON" % label) from None
        if attempt == MAX_ATTEMPTS:
            raise ApiAbort("GET %s: %s, giving up after %d attempts" % (label, problem, attempt))
        wait = _retry_wait(hdrs, attempt)
        if wait > MAX_WAIT:
            raise ApiAbort("GET %s: %s; server asks to wait %ds (MAX_WAIT is %ds)"
                           % (label, problem, wait, MAX_WAIT))
        log("WARNING: GET %s: %s; retrying in %ds (attempt %d/%d)"
            % (label, problem, wait, attempt, MAX_ATTEMPTS))
        time.sleep(wait)


def api_get(path):
    """Single parsed JSON object from an API path like '/repos/...'."""
    return _get_json(API + path)[0]


def api_paginate(path, key=None):
    """Concatenated items across pages (following Link rel="next"). For endpoints that
    wrap the list in an object, pass key (e.g. "check_runs") to collect that array."""
    items, url = [], API + path
    while url:
        data, url = _get_json(url)
        items.extend(data[key] if key else data)
    return items


def check_quota(pr_count):
    """Optional courtesy: stop early if the core quota can't cover one call per PR.
    /rate_limit itself does not count against the quota."""
    try:
        core = api_get("/rate_limit")["resources"]["core"]
        remaining, reset = int(core["remaining"]), int(core["reset"])
    except (ApiError, KeyError, TypeError, ValueError) as e:
        log("WARNING: could not read rate limit (continuing): %s" % e)
        return
    if remaining < pr_count:
        die("only %d API calls left (resets at %s), need at least %d for %d PRs; try later."
            % (remaining, datetime.fromtimestamp(reset, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               pr_count, pr_count))


def to_utc_z(value):
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ci_status(sha):
    runs = api_paginate("/repos/%s/commits/%s/check-runs?per_page=100" % (REPO, sha), "check_runs")
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
    st = api_get("/repos/%s/commits/%s/status" % (REPO, sha))
    if not st or not st.get("total_count"):
        return "none"
    return {"success": "passing", "failure": "failing", "error": "failing",
            "pending": "pending"}.get(st.get("state"), "none")


# Section headers from legacy `git merge-tree <base> <b1> <b2>` are followed by
# indented "base/our/their/result <mode> <sha> <path>" lines naming the file.
_MT_PATH_RE = re.compile(r"^  (?:base|our|their|result)\s+\d+\s+[0-9a-f]+\s+(.*)$")


def _legacy_conflicts(tmp, env, pr_ref):
    """Conflict detection for git < 2.38 (no `merge-tree --write-tree`).

    Runs the legacy 3-arg plumbing merge (`git merge-tree <merge-base> main pr`),
    which performs a real content-level 3-way merge and emits standard conflict
    markers (<<<<<<<) in the hunks of files that actually conflict; a file that
    merges cleanly shows none. The legacy command always exits 0, so conflicts
    are detected from the markers, not the exit code.
    """
    base = git(tmp, env, "merge-base", BASE, pr_ref)
    if base.returncode != 0 or not base.stdout.strip():
        return False, []  # unrelated histories / missing ref -> treat as unknown
    m = git(tmp, env, "merge-tree", base.stdout.strip(), BASE, pr_ref)
    if m.returncode != 0:
        log("WARNING: legacy merge-tree exit %d for %s; conflicts unknown: %s"
            % (m.returncode, pr_ref, m.stderr.strip()))
        return False, []
    files, current = [], None
    for line in m.stdout.split("\n"):
        path_match = _MT_PATH_RE.match(line)
        if path_match:
            current = path_match.group(1)
        elif current and "<<<<<<<" in line and current not in files:
            files.append(current)
    return bool(files), files


def detect_conflicts(tmp, env, pr_ref):
    """Local 3-way merge of a PR against BASE, used for has_conflicts/conflict_files.

    Prefers the modern `merge-tree --write-tree` (git >= 2.38); on older git the
    flag is rejected (exit 129) and we fall back to the legacy plumbing merge.
    GitHub's own `mergeable` flag is deliberately not used.
    """
    m = git(tmp, env, "merge-tree", "--write-tree", "--name-only", BASE, pr_ref)
    if m.returncode == 0:
        return False, []
    if m.returncode == 1:
        out = []
        for line in m.stdout.split("\n")[1:]:  # line 0 is the tree OID
            if line == "":
                break
            if line not in out:
                out.append(line)
        return True, out
    # exit 129 (old git without --write-tree) or other plumbing error -> legacy path
    return _legacy_conflicts(tmp, env, pr_ref)


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

    has_conflicts, conflict_files = detect_conflicts(tmp, env, pr_ref)

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
    if shutil.which("git") is None:
        die("`git` not found on PATH.")

    # git is anonymous (the repo is public): the PAT is kept out of its environment.
    # If the repo ever becomes private, git would need auth (e.g. a tokened remote URL
    # or http.extraHeader) - deliberately not added now.
    env = {k: v for k, v in os.environ.items() if k not in TOKEN_VARS + ("GITHUB_TOKEN",)}
    env["GIT_TERMINAL_PROMPT"] = "0"

    try:
        pulls = api_paginate("/repos/%s/pulls?state=open&per_page=100" % REPO)
    except (ApiError, ApiAbort) as e:
        die("could not list open PRs: %s" % e)
    log("Found %d open PRs." % len(pulls))
    check_quota(len(pulls))

    tmp = tempfile.mkdtemp(prefix="prdash_")
    atexit.register(rmtree_force, tmp)
    try:
        # Full history (no --depth) is required for merge-base detection.
        r = run(["git", "clone", "https://github.com/%s" % REPO, tmp], env)
        if r.returncode != 0:
            die("clone failed: " + r.stderr.strip())
        if git(tmp, env, "rev-parse", "--verify", "--quiet", BASE).returncode != 0:
            die("branch '%s' not found in clone." % BASE)
        f = git(tmp, env, "fetch", "origin", "+refs/pull/*/head:refs/remotes/pr/*")
        if f.returncode != 0:
            log("WARNING: fetching PR head refs reported errors (will fall back to head.sha): "
                + f.stderr.strip())

        rows = []
        for idx, pr in enumerate(pulls, 1):
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
                ci = ci_status(sha)
            except ApiAbort as e:
                die("aborting at PR #%s (%d of %d): %s" % (num, idx, len(pulls), e))
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
    except ApiAbort as exc:
        die(str(exc))
    except Exception as exc:
        die("unexpected failure: %s" % exc)
