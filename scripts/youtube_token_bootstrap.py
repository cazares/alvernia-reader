#!/usr/bin/env python3
from __future__ import annotations

"""
Bootstrap (or rotate) YouTube OAuth token locally for server-side Step5 uploads.

This script is intended to run on a developer machine with a browser.
It writes youtube_token.json and prints environment variable update commands.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return False

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except Exception as exc:
    print(
        "Missing google-auth-oauthlib. Install with:\n"
        "  ./.venv/bin/pip install -r requirements.txt\n"
        "or:\n"
        "  pip install google-auth-oauthlib",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc

YOUTUBE_UPLOAD_SCOPE = ["https://www.googleapis.com/auth/youtube.upload"]


def _resolve_client_secrets_path(raw: str) -> Path:
    candidate = Path(str(raw or "").strip()).expanduser()
    if not str(candidate):
        raise RuntimeError("Client secrets path is empty.")

    if candidate.is_file():
        return candidate

    if candidate.is_dir():
        guess = candidate / "client_secret.json"
        if guess.exists():
            return guess

    raise RuntimeError(f"Could not find client_secret.json from: {candidate}")


def _default_client_secrets_arg() -> str:
    raw = os.getenv("YOUTUBE_CLIENT_SECRETS_JSON", "").strip()
    if raw:
        return raw
    return str((Path.cwd() / "client_secret.json").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate youtube_token.json via local OAuth browser flow.")
    p.add_argument(
        "--client-secrets",
        default=_default_client_secrets_arg(),
        help="Path to client_secret.json or directory containing it.",
    )
    p.add_argument(
        "--token-out",
        default="",
        help="Output path for youtube_token.json (default: sibling of client_secret.json).",
    )
    p.add_argument(
        "--no-open-browser",
        action="store_true",
        help="Do not auto-open browser window (you can manually open the printed URL).",
    )
    p.add_argument(
        "--print-env-commands",
        dest="print_env_commands",
        action="store_true",
        default=True,
        help="Print recommended environment variable commands.",
    )
    p.add_argument(
        "--no-print-env-commands",
        dest="print_env_commands",
        action="store_false",
        help="Skip printing environment variable commands.",
    )
    return p.parse_args(argv)


def _run_local_oauth(client_secrets_path: Path, *, open_browser: bool):
    flow = InstalledAppFlow.from_client_secrets_file(
        str(client_secrets_path),
        scopes=YOUTUBE_UPLOAD_SCOPE,
    )

    # access_type=offline + prompt=consent helps ensure refresh_token is returned.
    kwargs = {
        "port": 0,
        "open_browser": bool(open_browser),
        "access_type": "offline",
        "prompt": "consent",
    }
    try:
        return flow.run_local_server(**kwargs)
    except TypeError:
        # Older library versions may not accept extra kwargs.
        kwargs.pop("access_type", None)
        kwargs.pop("prompt", None)
        return flow.run_local_server(**kwargs)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)

    try:
        client_secrets_path = _resolve_client_secrets_path(args.client_secrets)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    token_path = (
        Path(args.token_out).expanduser()
        if str(args.token_out or "").strip()
        else client_secrets_path.parent / "youtube_token.json"
    )
    token_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Client secrets: {client_secrets_path}")
    print(f"Token output  : {token_path}")
    print("Running OAuth login flow...")

    try:
        creds = _run_local_oauth(
            client_secrets_path,
            open_browser=(not bool(args.no_open_browser)),
        )
    except Exception as exc:
        print(f"ERROR: OAuth flow failed: {exc}", file=sys.stderr)
        return 1

    token_path.write_text(creds.to_json(), encoding="utf-8")
    try:
        token_path.chmod(0o600)
    except Exception:
        pass

    payload = json.loads(token_path.read_text(encoding="utf-8"))
    has_refresh_token = bool(payload.get("refresh_token"))

    print("Token written successfully.")
    print(f"Has refresh token: {has_refresh_token}")
    if not has_refresh_token:
        print(
            "WARNING: refresh_token is missing. Re-run and ensure consent is granted.",
            file=sys.stderr,
        )

    if args.print_env_commands:
        print("")
        print("Recommended environment variable updates:")
        print("export YOUTUBE_CLIENT_SECRETS_JSON=/tmp/client_secret.json")
        print(f"export YOUTUBE_CLIENT_SECRETS_JSON_RAW=< {client_secrets_path}")
        print(f"export YOUTUBE_TOKEN_JSON_RAW=< {token_path}")

    return 0 if has_refresh_token else 2


if __name__ == "__main__":
    raise SystemExit(main())

