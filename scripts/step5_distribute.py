#!/usr/bin/env python3
from __future__ import annotations
"""
Minimal YouTube uploader for Mixterioso.

Usage:
    python3 scripts/step5_distribute.py --slug mujer_hilandera

Requirements:
    - Environment variable YOUTUBE_CLIENT_SECRETS_JSON must point to:
        * client_secret.json  OR
        * a directory containing client_secret.json

    - OAuth token will be stored as youtube_token.json next to client_secret.json
"""

import argparse
import json
import os
import re
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return False

# Google API imports (optional until upload path is actually used)
_GOOGLE_IMPORT_ERROR: Optional[Exception] = None
try:
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
except Exception as _google_import_exc:
    _GOOGLE_IMPORT_ERROR = _google_import_exc
    InstalledAppFlow = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]
    HttpError = Exception  # type: ignore[assignment]
    MediaFileUpload = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]

# ─────────────────────────────────────────────
# Bootstrap sys.path for scripts.common import
# ─────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common import (
    log, CYAN, GREEN, YELLOW, RED,
    resolve_output_dir,
    slugify,
)
from scripts.sync_quality import merge_sync_check_runs, run_sync_quality_checks

OUT_DIR  = resolve_output_dir(ROOT)
META_DIR = ROOT / "meta"
TIMINGS_DIR = ROOT / "timings"

def read_json(path: Path) -> Optional[dict]:
    try:
        import json as _json
        return _json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

NON_INTERACTIVE = False
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    cleaned = str(raw).strip().lower()
    if cleaned in _TRUE_VALUES:
        return True
    if cleaned in _FALSE_VALUES:
        return False
    return bool(default)


def ask_yes_no(prompt: str, default_yes: bool = True) -> bool:
    suffix = "[Y/n]" if default_yes else "[y/N]"
    try:
        ans = input(f"{prompt} {suffix}: ").strip().lower()
    except EOFError:
        ans = ""
    if ans == "" and default_yes:
        return True
    if ans == "" and not default_yes:
        return False
    return ans in ("y", "yes")


def _allow_oauth_login_flow(*, non_interactive: bool) -> bool:
    """Decide whether browser OAuth flow is allowed for this invocation."""
    raw = os.getenv("MIXTERIOSO_YOUTUBE_ALLOW_BROWSER_OAUTH")
    if raw is not None and raw.strip():
        return raw.strip().lower() in _TRUE_VALUES

    if non_interactive:
        return False

    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


def _oauth_repair_instructions(secrets_path: Path, token_path: Path, *, reason: str) -> str:
    return "\n".join(
        [
            f"YouTube OAuth credentials are not usable ({reason}).",
            "Browser OAuth flow is disabled for this run, so Step5 cannot open a login screen.",
            "Repair from a local machine, then update deployment secrets:",
            "  1) python3 scripts/youtube_token_bootstrap.py --client-secrets /absolute/path/client_secret.json",
            "  2) Set YOUTUBE_CLIENT_SECRETS_JSON=/tmp/client_secret.json",
            "  3) Set YOUTUBE_CLIENT_SECRETS_JSON_RAW from /absolute/path/client_secret.json",
            "  4) Set YOUTUBE_TOKEN_JSON_RAW from /absolute/path/youtube_token.json",
            "  5) Redeploy service",
            f"Current server paths: secrets={secrets_path} token={token_path}",
            "If you intentionally want browser auth on this host, set MIXTERIOSO_YOUTUBE_ALLOW_BROWSER_OAUTH=1.",
        ]
    )

def open_path(path: Path) -> None:
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", str(path)])
        elif sys.platform.startswith("win"):
            subprocess.run(["start", str(path)], shell=True)
        else:
            subprocess.run(["xdg-open", str(path)])
    except Exception as e:
        log("OPEN", f"Failed to open {path}: {e}", YELLOW)

# Load .env (for YOUTUBE_CLIENT_SECRETS_JSON, etc.)
load_dotenv()

# Scope required for uploading videos
YOUTUBE_UPLOAD_SCOPE = ["https://www.googleapis.com/auth/youtube.upload"]


def _require_upload_deps() -> None:
    if _GOOGLE_IMPORT_ERROR is None:
        return
    raise RuntimeError(
        "Missing Step5 upload dependencies. Install with:\n"
        "  ./.venv/bin/pip install -r requirements.txt\n"
        "or:\n"
        "  pip install python-dotenv google-api-python-client google-auth-oauthlib"
    )


# ─────────────────────────────────────────────
# Secrets / OAuth helpers
# ─────────────────────────────────────────────
def load_secrets_path() -> Path:
    """
    Resolve the location of client_secret.json based on YOUTUBE_CLIENT_SECRETS_JSON.

    Accepts:
      - exact file path to client_secret.json
      - directory containing client_secret.json
    """
    raw = os.getenv("YOUTUBE_CLIENT_SECRETS_JSON")

    if not raw:
        raise RuntimeError("YOUTUBE_CLIENT_SECRETS_JSON is not set.")

    p = Path(raw).expanduser()

    if p.is_file():
        return p

    if p.is_dir():
        guess = p / "client_secret.json"
        if guess.exists():
            return guess

    raise RuntimeError(f"Invalid secrets path: {p}")


def get_credentials(secrets_path: Path, *, allow_oauth_login_flow: bool = True):
    """
    Get or create OAuth credentials for the YouTube upload scope.

    Token is stored as youtube_token.json next to client_secret.json.
    """
    _require_upload_deps()

    token_path = secrets_path.parent / "youtube_token.json"
    creds = None
    token_load_error = ""

    # Try to load existing token
    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(
                str(token_path),
                YOUTUBE_UPLOAD_SCOPE,
            )
        except Exception as exc:
            token_load_error = str(exc)
            creds = None

    # Refresh first whenever possible
    if creds and not creds.valid and creds.expired and creds.refresh_token:
        try:
            log("OAUTH", "Refreshing existing OAuth token...", CYAN)
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
            log("OAUTH", f"Saved refreshed OAuth token to {token_path}", GREEN)
        except Exception as exc:
            token_load_error = str(exc)
            creds = None

    if creds and creds.valid:
        return creds

    if not allow_oauth_login_flow:
        reason = (
            "token file missing"
            if not token_path.exists()
            else ("token refresh failed" if token_load_error else "token invalid")
        )
        raise RuntimeError(
            _oauth_repair_instructions(
                secrets_path,
                token_path,
                reason=reason,
            )
        )

    log("OAUTH", "Running OAuth login flow...", CYAN)
    flow = InstalledAppFlow.from_client_secrets_file(
        str(secrets_path),
        scopes=YOUTUBE_UPLOAD_SCOPE,
    )
    # This opens a browser and listens on localhost
    creds = flow.run_local_server(port=0)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    log("OAUTH", f"Saved OAuth token to {token_path}", GREEN)
    return creds


# ─────────────────────────────────────────────
# Thumbnail helper
# ─────────────────────────────────────────────
def extract_thumbnail(video_path: Path, out_path: Path, time_sec: float) -> None:
    """
    Extract a JPEG thumbnail from the given time position using ffmpeg.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(time_sec),
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(out_path),
    ]
    log("THUMB", " ".join(cmd), CYAN)
    subprocess.run(cmd, check=True)


# ─────────────────────────────────────────────
# Upload logic
# ─────────────────────────────────────────────
def upload_video(
    youtube,
    video_path: Path,
    title: str,
    description: str,
    tags: list[str],
    category_id: str,
    privacy: str,
) -> str:
    """
    Perform the actual YouTube upload and return the new video ID.
    """
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }

    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True)

    log("UPLOAD", f"Starting upload: {video_path}", CYAN)
    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None
    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                log("UPLOAD", f"Progress: {pct}%", CYAN)
        except HttpError as e:
            log("ERROR", f"Upload failed: {e}", RED)
            raise

    video_id = response.get("id")
    log("UPLOAD", f"Upload complete. video_id={video_id}", GREEN)
    return video_id


def set_thumbnail(youtube, video_id: str, thumb_path: Path) -> None:
    """
    Upload a thumbnail for a video.
    """
    log("THUMB", f"Uploading thumbnail for {video_id}: {thumb_path}", CYAN)
    media = MediaFileUpload(str(thumb_path), mimetype="image/jpeg")
    request = youtube.thumbnails().set(videoId=video_id, media_body=media)
    _ = request.execute()
    log("THUMB", "Thumbnail set.", GREEN)


# ─────────────────────────────────────────────
# Title / meta helpers
# ─────────────────────────────────────────────
def _infer_artist_title_from_lrc(slug: str) -> Optional[dict]:
    """Try to read [ar:...] and [ti:...] tags from timings/<slug>.lrc."""
    lrc_path = TIMINGS_DIR / f"{slug}.lrc"
    if not lrc_path.exists():
        return None

    artist = ""
    title = ""

    tag_re = re.compile(r"^\[([a-zA-Z]{2,10})\s*:\s*(.*?)\s*\]\s*$")
    try:
        lines = lrc_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None

    for raw in lines[:60]:
        s = raw.strip()
        if not s.startswith("[") or "]" not in s:
            continue
        m = tag_re.match(s)
        if not m:
            continue
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if not val:
            continue

        if key in ("ar", "artist", "art"):
            artist = val
        elif key in ("ti", "title"):
            title = val
        elif key in ("au", "author") and not artist:
            artist = val

        if artist and title:
            break

    if not (artist or title):
        return None

    return {"artist": artist, "title": title, "_meta_path": str(lrc_path)}


def load_meta_for_slug(slug: str) -> Optional[dict]:
    """Load best-effort metadata for a slug."""
    candidates = [
        META_DIR / f"{slug}.json",
        META_DIR / f"{slug}.step1.json",
        META_DIR / f"{slug}.step2.json",
        META_DIR / f"{slug}.step3.json",
        META_DIR / f"{slug}.step4.json",
        META_DIR / f"{slug}.step5.json",
    ]

    best_any: Optional[dict] = None

    for p in candidates:
        if p.exists():
            j = read_json(p)
            knows = isinstance(j, dict)
            if knows:
                j["_meta_path"] = str(p)
                artist = (j.get("artist") or "").strip()
                title = (j.get("title") or "").strip()
                if artist and title:
                    return j
                if best_any is None:
                    best_any = j

    try:
        extras = sorted(
            META_DIR.glob(f"{slug}*.json"),
            key=lambda pp: pp.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        extras = []

    for p in extras:
        if str(p) in {str(x) for x in candidates}:
            continue
        j = read_json(p)
        if isinstance(j, dict):
            j["_meta_path"] = str(p)
            artist = (j.get("artist") or "").strip()
            title = (j.get("title") or "").strip()
            if artist and title:
                return j
            if best_any is None:
                best_any = j

    if best_any is not None:
        return best_any

    return _infer_artist_title_from_lrc(slug)


def auto_main_title(slug: str, meta: Optional[dict]) -> str:
    """Return base title in the required format: 'Artist - Title' when possible."""
    if isinstance(meta, dict):
        artist = (meta.get("artist") or "").strip()
        title = (meta.get("title") or "").strip()
        if artist and title:
            return f"{artist} - {title}"
        if title:
            return title
    return slug.replace("_", " ").title()


def build_tags(meta: Optional[dict]) -> list[str]:
    """Simple, predictable tags."""
    tags = ["karaoke", "lyrics"]
    if isinstance(meta, dict):
        artist = (meta.get("artist") or "").strip()
        title  = (meta.get("title") or "").strip()
        if artist:
            tags.append(artist)
        if title:
            tags.append(title)

    seen = set()
    out = []
    for t in tags:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _parse_percent(value) -> int | None:
    """Parse a percent-ish value into int 0..100, or None if unknown."""
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        v = float(value)
        if 0.0 <= v <= 1.0:
            return int(round(v * 100))
        if 0.0 <= v <= 100.0:
            return int(round(v))
        return None

    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return None
        m = re.search(r"(\d{1,3})\s*%?", s)
        if m:
            try:
                n = int(m.group(1))
                if 0 <= n <= 100:
                    return n
            except Exception:
                return None
    return None


def _find_first_percent(meta: dict, candidates: list[str]) -> int | None:
    for k in candidates:
        if k in meta:
            p = _parse_percent(meta.get(k))
            if p is not None:
                if "reduc" in k and "level" not in k and "pct" in k:
                    return max(0, min(100, 100 - p))
                return p
    return None


def _infer_stem_pcts(meta: Optional[dict]) -> tuple[int | None, int | None]:
    """Return (vocals_pct, bass_pct) if discoverable from meta."""
    if not isinstance(meta, dict):
        return (None, None)

    vocals_keys = [
        "vocals_pct", "vocals_percent", "vocals_percentage",
        "vocals_level_pct", "vocals_level", "vocals_volume",
        "vocals_gain_pct", "vocals_mix_pct",
        "reduced_vocals_pct", "vocals_reduction_pct", "reduce_vocals_pct",
    ]

    bass_keys = [
        "bass_pct", "bass_percent", "bass_percentage",
        "bass_level_pct", "bass_level", "bass_volume",
        "bass_gain_pct", "bass_mix_pct",
        "reduced_bass_pct", "bass_reduction_pct", "reduce_bass_pct",
    ]

    vocals_pct = _find_first_percent(meta, vocals_keys)
    bass_pct = _find_first_percent(meta, bass_keys)

    for container_key in ("stems", "stem_levels", "mix", "levels"):
        container = meta.get(container_key)
        if isinstance(container, dict):
            if vocals_pct is None:
                vocals_pct = _find_first_percent(container, vocals_keys + ["vocals"])
            if bass_pct is None:
                bass_pct = _find_first_percent(container, bass_keys + ["bass"])

    return (vocals_pct, bass_pct)


def suggest_ending_from_stems(meta: Optional[dict]) -> str | None:
    """Build an ending like: '35% Vocals, No Bass' or 'Karaoke'."""
    vocals_pct, bass_pct = _infer_stem_pcts(meta)

    parts: list[str] = []

    if vocals_pct is not None:
        if vocals_pct == 0:
            parts.append("Karaoke")
        else:
            parts.append(f"{vocals_pct}% Vocals")

    if bass_pct is not None:
        if bass_pct == 0:
            parts.append("No Bass")

    if not parts:
        return None
    return ", ".join(parts)


def choose_title(slug: str, meta: Optional[dict]) -> str:
    return choose_title_with_options(slug, meta, ending="", interactive=True)


def choose_title_with_options(
    slug: str,
    meta: Optional[dict],
    *,
    ending: str = "",
    interactive: bool = True,
) -> str:
    main_title = auto_main_title(slug, meta)
    suggested = suggest_ending_from_stems(meta)
    chosen_ending = (ending or "").strip()

    if chosen_ending:
        return f"{main_title} ({chosen_ending})"

    if not interactive:
        fallback = suggested or "Karaoke"
        return f"{main_title} ({fallback})"

    print()
    print(f"Base title: {main_title}")
    if suggested:
        print(f"Suggested ending: {suggested}")
    print()

    while True:
        try:
            raw = input("Custom ending (Enter for suggested/default): ").strip()
        except EOFError:
            raw = ""
        if raw:
            return f"{main_title} ({raw})"
        if suggested:
            return f"{main_title} ({suggested})"
        return f"{main_title} (Karaoke)"


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Upload Mixterioso MP4 to YouTube (minimal interface).")

    p.add_argument(
        "--slug",
        required=True,
        help="Slug for the song (e.g. 'mujer_hilandera').",
    )
    p.add_argument(
        "--privacy",
        choices=["public", "unlisted", "private"],
        default="unlisted",
        help="Privacy status for the video (default: unlisted).",
    )
    p.add_argument(
        "--title",
        default="",
        help="Full YouTube title override. If omitted, title is auto-generated from meta.",
    )
    p.add_argument(
        "--ending",
        default="",
        help="Optional ending appended as '(...)' when --title is not provided.",
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Skip upload confirmation prompt.",
    )
    p.add_argument(
        "--non-interactive",
        action="store_true",
        help="Disable interactive prompts and auto-select title ending.",
    )
    p.add_argument(
        "--open-output-dir",
        action="store_true",
        help="Open output directory after upload completes.",
    )

    return p.parse_args(argv)


def _resolve_video_path(slug: str) -> Path:
    direct = OUT_DIR / f"{slug}.mp4"
    if direct.exists():
        return direct

    matches = sorted(OUT_DIR.glob(f"{slug}*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if matches:
        log("VIDEO", f"MP4 not found at {direct}; using newest match: {matches[0]}", YELLOW)
        return matches[0]

    return direct


def _write_step5_meta(
    slug: str,
    *,
    video_id: str,
    video_url: str,
    title: str,
    privacy: str,
    tags: list[str],
    video_path: Path,
    sync_checks: Optional[dict] = None,
) -> Path:
    payload = {
        "slug": slug,
        "video_id": video_id,
        "video_url": video_url,
        "title": title,
        "privacy": privacy,
        "tags": list(tags),
        "video_path": str(video_path),
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(sync_checks, dict) and sync_checks:
        payload["sync_checks"] = sync_checks
    out_path = META_DIR / f"{slug}.step5.json"
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return out_path


def _log_sync_check_scope(label: str, scope: Optional[dict]) -> None:
    if not isinstance(scope, dict):
        return
    passed = scope.get("passed")
    elapsed_sec = float(scope.get("elapsed_sec") or 0.0)
    checks = scope.get("checks") if isinstance(scope.get("checks"), dict) else {}
    if passed is True:
        scope_color = GREEN
        scope_status = "passed"
    elif passed is False:
        scope_color = RED
        scope_status = "failed"
    else:
        scope_color = YELLOW
        scope_status = "skipped"
    log("SYNC", f"{label}: {scope_status} ({elapsed_sec:.2f}s)", scope_color)
    for key, result in checks.items():
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "skipped")
        check_elapsed = float(result.get("elapsed_sec") or 0.0)
        reason = str(result.get("reason") or "").strip()
        if status == "passed":
            color = GREEN
        elif status == "failed":
            color = RED
        else:
            color = YELLOW
        suffix = f" [{reason}]" if reason else ""
        log("SYNC", f"  - {key}: {status} ({check_elapsed:.2f}s){suffix}", color)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    slug = slugify(args.slug)
    sync_checks_enabled = _env_bool("MIXTERIOSO_SYNC_CHECKS_ENABLED", False)
    sync_checks_block_on_fail = _env_bool("MIXTERIOSO_SYNC_CHECKS_BLOCK_ON_FAIL", False)
    sync_checks_pre_enabled = _env_bool("MIXTERIOSO_SYNC_CHECK_PRE_UPLOAD", True)
    sync_checks_post_enabled = _env_bool("MIXTERIOSO_SYNC_CHECK_POST_UPLOAD", True)
    sync_checks_payload: Optional[dict] = None
    try:
        video_path = _resolve_video_path(slug)
        if not video_path.exists():
            log("ERROR", f"MP4 file not found: {video_path}", RED)
            return 1

        meta = load_meta_for_slug(slug)
        if meta:
            src = meta.get("_meta_path") if isinstance(meta, dict) else None
            log("META", f"Loaded meta for '{slug}'" + (f" ({src})" if src else ""), CYAN)
        else:
            log("META", f"No meta JSON found for '{slug}'", YELLOW)

        if args.title.strip():
            title = args.title.strip()
        else:
            title = choose_title_with_options(
                slug,
                meta,
                ending=args.ending,
                interactive=not bool(args.non_interactive),
            )

        description = ""
        tags = build_tags(meta)

        print()
        log("SUMMARY", "YouTube upload configuration:", CYAN)
        print(f"  File      : {video_path}")
        print(f"  Title     : {title}")
        print(f"  Privacy   : {args.privacy}")
        print(f"  Tags      : {', '.join(tags) if tags else '(none)'}")
        print(f"  Description length: {len(description)} chars")
        print()

        should_confirm = not (bool(args.yes) or bool(args.non_interactive))
        if should_confirm and not ask_yes_no("Proceed with upload?", default_yes=True):
            log("ABORT", "User cancelled upload.", YELLOW)
            return 0

        if sync_checks_enabled and sync_checks_pre_enabled:
            log("SYNC", "Running pre-upload sync checks", CYAN)
            try:
                pre_run = run_sync_quality_checks(
                    slug=slug,
                    local_video_path=video_path,
                    run_pre_upload=True,
                    run_post_upload=False,
                    language="auto",
                )
                sync_checks_payload = merge_sync_check_runs(sync_checks_payload, pre_run)
                _log_sync_check_scope("Pre-upload", pre_run.get("pre_upload"))
                if sync_checks_block_on_fail and pre_run.get("overall_passed") is False:
                    log("SYNC", "Blocking upload because pre-upload sync checks failed", RED)
                    return 1
            except Exception as e:
                log("SYNC", f"Pre-upload sync checks crashed: {e}", YELLOW)

        _require_upload_deps()
        if build is None:
            raise RuntimeError(
                "Google API dependencies are missing. "
                "Install: pip install python-dotenv google-api-python-client google-auth-oauthlib"
            )

        secrets_path = load_secrets_path()
        allow_oauth_login_flow = _allow_oauth_login_flow(non_interactive=bool(args.non_interactive))
        if not allow_oauth_login_flow:
            log("OAUTH", "Browser OAuth login disabled (refresh-token-only mode).", CYAN)
        creds = get_credentials(
            secrets_path,
            allow_oauth_login_flow=allow_oauth_login_flow,
        )
        youtube = build("youtube", "v3", credentials=creds)

        video_id = upload_video(
            youtube,
            video_path,
            title,
            description,
            tags,
            category_id="10",  # Music
            privacy=args.privacy,
        )
        video_url = f"https://youtube.com/watch?v={video_id}"

        thumb_path = video_path.with_suffix(".jpg")
        try:
            extract_thumbnail(video_path, thumb_path, time_sec=0.5)
            set_thumbnail(youtube, video_id, thumb_path)
        except Exception as e:
            log("THUMB", f"Thumbnail failed: {e}", YELLOW)

        if sync_checks_enabled and sync_checks_post_enabled:
            log("SYNC", "Running post-upload sync checks", CYAN)
            try:
                post_run = run_sync_quality_checks(
                    slug=slug,
                    local_video_path=video_path,
                    youtube_video_url=video_url,
                    run_pre_upload=False,
                    run_post_upload=True,
                    language="auto",
                )
                sync_checks_payload = merge_sync_check_runs(sync_checks_payload, post_run)
                _log_sync_check_scope("Post-upload", post_run.get("post_upload"))
                if sync_checks_block_on_fail and post_run.get("overall_passed") is False:
                    log("SYNC", "Post-upload sync checks failed", RED)
                    return 1
            except Exception as e:
                log("SYNC", f"Post-upload sync checks crashed: {e}", YELLOW)

        try:
            step5_meta_path = _write_step5_meta(
                slug,
                video_id=video_id,
                video_url=video_url,
                title=title,
                privacy=args.privacy,
                tags=tags,
                video_path=video_path,
                sync_checks=sync_checks_payload,
            )
            log("META", f"Wrote {step5_meta_path}", GREEN)
        except Exception as e:
            log("META", f"Failed to write step5 metadata: {e}", YELLOW)

        log("DONE", f"Video available at: {video_url}", GREEN)

        if args.open_output_dir:
            open_path(OUT_DIR)
        return 0
    except Exception as e:
        log("ERROR", str(e), RED)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
