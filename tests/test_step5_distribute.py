import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


def _install_step5_import_stubs() -> None:
    # dotenv
    dotenv_mod = sys.modules.get("dotenv") or types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    sys.modules["dotenv"] = dotenv_mod

    # google_auth_oauthlib.flow
    ga_pkg = sys.modules.get("google_auth_oauthlib") or types.ModuleType("google_auth_oauthlib")
    ga_flow_mod = sys.modules.get("google_auth_oauthlib.flow") or types.ModuleType("google_auth_oauthlib.flow")

    class DummyInstalledAppFlow:
        @classmethod
        def from_client_secrets_file(cls, *args, **kwargs):  # type: ignore[no-untyped-def]
            return cls()

        def run_local_server(self, port=0):  # type: ignore[no-untyped-def]
            class _Creds:
                valid = True
                expired = False
                refresh_token = None

                def to_json(self) -> str:
                    return "{}"

            return _Creds()

    ga_flow_mod.InstalledAppFlow = DummyInstalledAppFlow  # type: ignore[attr-defined]
    ga_pkg.flow = ga_flow_mod  # type: ignore[attr-defined]
    sys.modules["google_auth_oauthlib"] = ga_pkg
    sys.modules["google_auth_oauthlib.flow"] = ga_flow_mod

    # googleapiclient.*
    gap_pkg = sys.modules.get("googleapiclient") or types.ModuleType("googleapiclient")
    gap_discovery = sys.modules.get("googleapiclient.discovery") or types.ModuleType("googleapiclient.discovery")
    gap_errors = sys.modules.get("googleapiclient.errors") or types.ModuleType("googleapiclient.errors")
    gap_http = sys.modules.get("googleapiclient.http") or types.ModuleType("googleapiclient.http")

    gap_discovery.build = lambda *args, **kwargs: object()  # type: ignore[attr-defined]

    class DummyHttpError(Exception):
        pass

    class DummyMediaFileUpload:
        def __init__(self, filename, mimetype=None, resumable=False):  # type: ignore[no-untyped-def]
            self.filename = filename
            self.mimetype = mimetype
            self.resumable = resumable

    gap_errors.HttpError = DummyHttpError  # type: ignore[attr-defined]
    gap_http.MediaFileUpload = DummyMediaFileUpload  # type: ignore[attr-defined]
    gap_pkg.discovery = gap_discovery  # type: ignore[attr-defined]
    gap_pkg.errors = gap_errors  # type: ignore[attr-defined]
    gap_pkg.http = gap_http  # type: ignore[attr-defined]
    sys.modules["googleapiclient"] = gap_pkg
    sys.modules["googleapiclient.discovery"] = gap_discovery
    sys.modules["googleapiclient.errors"] = gap_errors
    sys.modules["googleapiclient.http"] = gap_http

    # google.oauth2.credentials
    google_pkg = sys.modules.get("google") or types.ModuleType("google")
    google_oauth2 = sys.modules.get("google.oauth2") or types.ModuleType("google.oauth2")
    google_oauth2_credentials = sys.modules.get("google.oauth2.credentials") or types.ModuleType("google.oauth2.credentials")

    class DummyCredentials:
        def __init__(self, valid=True, expired=False, refresh_token=None):
            self.valid = valid
            self.expired = expired
            self.refresh_token = refresh_token

        @classmethod
        def from_authorized_user_file(cls, *args, **kwargs):  # type: ignore[no-untyped-def]
            return cls(valid=True, expired=False, refresh_token="rt")

        def refresh(self, request):  # type: ignore[no-untyped-def]
            self.valid = True

        def to_json(self) -> str:
            return '{"token":"x"}'

    google_oauth2_credentials.Credentials = DummyCredentials  # type: ignore[attr-defined]
    google_pkg.oauth2 = google_oauth2  # type: ignore[attr-defined]
    google_oauth2.credentials = google_oauth2_credentials  # type: ignore[attr-defined]
    sys.modules["google"] = google_pkg
    sys.modules["google.oauth2"] = google_oauth2
    sys.modules["google.oauth2.credentials"] = google_oauth2_credentials

    # google.auth.transport.requests
    google_auth = sys.modules.get("google.auth") or types.ModuleType("google.auth")
    google_auth_transport = sys.modules.get("google.auth.transport") or types.ModuleType("google.auth.transport")
    google_auth_transport_requests = sys.modules.get("google.auth.transport.requests") or types.ModuleType(
        "google.auth.transport.requests"
    )

    class DummyRequest:
        pass

    google_auth_transport_requests.Request = DummyRequest  # type: ignore[attr-defined]
    google_auth.transport = google_auth_transport  # type: ignore[attr-defined]
    google_auth_transport.requests = google_auth_transport_requests  # type: ignore[attr-defined]
    sys.modules["google.auth"] = google_auth
    sys.modules["google.auth.transport"] = google_auth_transport
    sys.modules["google.auth.transport.requests"] = google_auth_transport_requests


_install_step5_import_stubs()
from scripts import step5_distribute as step5  # noqa: E402


class Step5DistributeTests(unittest.TestCase):
    def test_read_json_valid_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            good = Path(td) / "good.json"
            bad = Path(td) / "bad.json"
            good.write_text('{"a":1}', encoding="utf-8")
            bad.write_text("{bad", encoding="utf-8")
            self.assertEqual(step5.read_json(good), {"a": 1})
            self.assertIsNone(step5.read_json(bad))

    def test_ask_yes_no_defaults_and_answers(self) -> None:
        with mock.patch("builtins.input", return_value=""):
            self.assertTrue(step5.ask_yes_no("Proceed?", default_yes=True))
        with mock.patch("builtins.input", return_value=""):
            self.assertFalse(step5.ask_yes_no("Proceed?", default_yes=False))
        with mock.patch("builtins.input", return_value="yes"):
            self.assertTrue(step5.ask_yes_no("Proceed?", default_yes=False))
        with mock.patch("builtins.input", return_value="n"):
            self.assertFalse(step5.ask_yes_no("Proceed?", default_yes=True))

    def test_open_path_uses_expected_command_for_platform(self) -> None:
        with mock.patch("scripts.step5_distribute.subprocess.run") as run_mock, mock.patch("scripts.step5_distribute.sys.platform", "darwin"):
            step5.open_path(Path("/tmp/a"))
            run_mock.assert_called_once_with(["open", "/tmp/a"])

        with mock.patch("scripts.step5_distribute.subprocess.run") as run_mock, mock.patch("scripts.step5_distribute.sys.platform", "win32"):
            step5.open_path(Path("C:/x"))
            run_mock.assert_called_once()
            args, kwargs = run_mock.call_args
            self.assertEqual(args[0], ["start", "C:/x"])
            self.assertTrue(kwargs["shell"])

    def test_load_secrets_path_accepts_file_or_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            secret = td_path / "client_secret.json"
            secret.write_text("{}", encoding="utf-8")

            with mock.patch.dict("os.environ", {"YOUTUBE_CLIENT_SECRETS_JSON": str(secret)}, clear=False):
                self.assertEqual(step5.load_secrets_path(), secret)

            with mock.patch.dict("os.environ", {"YOUTUBE_CLIENT_SECRETS_JSON": str(td_path)}, clear=False):
                self.assertEqual(step5.load_secrets_path(), secret)

    def test_load_secrets_path_errors_when_missing_or_invalid(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError):
                step5.load_secrets_path()

        with tempfile.TemporaryDirectory() as td:
            bad = Path(td) / "nope"
            with mock.patch.dict("os.environ", {"YOUTUBE_CLIENT_SECRETS_JSON": str(bad)}, clear=False):
                with self.assertRaises(RuntimeError):
                    step5.load_secrets_path()

    def test_get_credentials_uses_existing_valid_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            secrets = Path(td) / "client_secret.json"
            token = Path(td) / "youtube_token.json"
            secrets.write_text("{}", encoding="utf-8")
            token.write_text("{}", encoding="utf-8")

            fake_creds = types.SimpleNamespace(valid=True, expired=False, refresh_token="rt")
            with mock.patch("scripts.step5_distribute.Credentials.from_authorized_user_file", return_value=fake_creds) as load_mock, mock.patch(
                "scripts.step5_distribute.InstalledAppFlow.from_client_secrets_file"
            ) as flow_mock:
                creds = step5.get_credentials(secrets)

            self.assertIs(creds, fake_creds)
            load_mock.assert_called_once()
            flow_mock.assert_not_called()

    def test_get_credentials_runs_flow_and_writes_token_when_needed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            secrets = Path(td) / "client_secret.json"
            token = Path(td) / "youtube_token.json"
            secrets.write_text("{}", encoding="utf-8")
            if token.exists():
                token.unlink()

            class FlowCreds:
                valid = True
                expired = False
                refresh_token = None

                def to_json(self) -> str:
                    return '{"token":"new"}'

            flow_obj = types.SimpleNamespace(run_local_server=lambda port=0: FlowCreds())
            with mock.patch("scripts.step5_distribute.InstalledAppFlow.from_client_secrets_file", return_value=flow_obj):
                creds = step5.get_credentials(secrets)
            self.assertTrue(token.exists())
            self.assertEqual(json.loads(token.read_text(encoding="utf-8")), {"token": "new"})
            self.assertTrue(creds.valid)

    def test_get_credentials_refreshes_and_persists_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            secrets = Path(td) / "client_secret.json"
            token = Path(td) / "youtube_token.json"
            secrets.write_text("{}", encoding="utf-8")
            token.write_text('{"token":"old"}', encoding="utf-8")

            class RefreshCreds:
                def __init__(self) -> None:
                    self.valid = False
                    self.expired = True
                    self.refresh_token = "rt"

                def refresh(self, _request) -> None:  # type: ignore[no-untyped-def]
                    self.valid = True

                def to_json(self) -> str:
                    return '{"token":"new","refresh_token":"rt"}'

            creds_obj = RefreshCreds()
            with (
                mock.patch("scripts.step5_distribute.Credentials.from_authorized_user_file", return_value=creds_obj),
                mock.patch("scripts.step5_distribute.InstalledAppFlow.from_client_secrets_file") as flow_mock,
            ):
                got = step5.get_credentials(secrets, allow_oauth_login_flow=False)

            self.assertIs(got, creds_obj)
            self.assertEqual(
                json.loads(token.read_text(encoding="utf-8")),
                {"token": "new", "refresh_token": "rt"},
            )
            flow_mock.assert_not_called()

    def test_get_credentials_raises_when_flow_disallowed_and_token_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            secrets = Path(td) / "client_secret.json"
            secrets.write_text("{}", encoding="utf-8")

            with self.assertRaises(RuntimeError) as ctx:
                step5.get_credentials(secrets, allow_oauth_login_flow=False)

        msg = str(ctx.exception)
        self.assertIn("Browser OAuth flow is disabled", msg)
        self.assertIn("youtube_token.json", msg)

    def test_infer_artist_title_from_lrc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings = Path(td) / "timings"
            timings.mkdir(parents=True, exist_ok=True)
            (timings / "song.lrc").write_text("[ar:Artist]\n[ti:Title]\n[00:01.00]Line\n", encoding="utf-8")
            with mock.patch.object(step5, "TIMINGS_DIR", timings):
                meta = step5._infer_artist_title_from_lrc("song")
        self.assertEqual(meta["artist"], "Artist")
        self.assertEqual(meta["title"], "Title")

    def test_load_meta_for_slug_prefers_meta_with_artist_and_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td) / "meta"
            timings = Path(td) / "timings"
            meta_dir.mkdir(parents=True, exist_ok=True)
            timings.mkdir(parents=True, exist_ok=True)
            (meta_dir / "song.step1.json").write_text('{"artist":"A","title":"T"}', encoding="utf-8")
            (meta_dir / "song.json").write_text('{"artist":"","title":"Partial"}', encoding="utf-8")
            with mock.patch.object(step5, "META_DIR", meta_dir), mock.patch.object(step5, "TIMINGS_DIR", timings):
                meta = step5.load_meta_for_slug("song")
        self.assertEqual(meta["artist"], "A")
        self.assertEqual(meta["title"], "T")

    def test_load_meta_for_slug_falls_back_to_lrc_when_no_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td) / "meta"
            timings = Path(td) / "timings"
            meta_dir.mkdir(parents=True, exist_ok=True)
            timings.mkdir(parents=True, exist_ok=True)
            (timings / "song.lrc").write_text("[ar:Artist]\n[ti:Title]\n", encoding="utf-8")
            with mock.patch.object(step5, "META_DIR", meta_dir), mock.patch.object(step5, "TIMINGS_DIR", timings):
                meta = step5.load_meta_for_slug("song")
        self.assertEqual(meta["artist"], "Artist")
        self.assertEqual(meta["title"], "Title")

    def test_auto_main_title(self) -> None:
        self.assertEqual(step5.auto_main_title("song_slug", {"artist": "A", "title": "T"}), "A - T")
        self.assertEqual(step5.auto_main_title("song_slug", {"artist": "", "title": "T"}), "T")
        self.assertEqual(step5.auto_main_title("song_slug", None), "Song Slug")

    def test_build_tags_dedupes(self) -> None:
        self.assertEqual(step5.build_tags(None), ["karaoke", "lyrics"])
        tags = step5.build_tags({"artist": "A", "title": "A"})
        self.assertEqual(tags, ["karaoke", "lyrics", "A"])

    def test_parse_percent(self) -> None:
        self.assertIsNone(step5._parse_percent(None))
        self.assertIsNone(step5._parse_percent(True))
        self.assertEqual(step5._parse_percent(0.5), 50)
        self.assertEqual(step5._parse_percent(35), 35)
        self.assertEqual(step5._parse_percent("70%"), 70)
        self.assertIsNone(step5._parse_percent("500%"))

    def test_find_first_percent_handles_reduction_key(self) -> None:
        meta = {"reduced_vocals_pct": 35}
        got = step5._find_first_percent(meta, ["reduced_vocals_pct"])
        self.assertEqual(got, 65)

    def test_infer_stem_pcts_from_top_level_and_nested_maps(self) -> None:
        self.assertEqual(step5._infer_stem_pcts(None), (None, None))
        self.assertEqual(step5._infer_stem_pcts({"vocals_pct": 40, "bass_pct": 0}), (40, 0))
        self.assertEqual(step5._infer_stem_pcts({"stems": {"vocals": 0.35, "bass": "0%"}}), (35, 0))

    def test_suggest_ending_from_stems(self) -> None:
        self.assertIsNone(step5.suggest_ending_from_stems(None))
        self.assertEqual(step5.suggest_ending_from_stems({"vocals_pct": 0}), "Karaoke")
        self.assertEqual(step5.suggest_ending_from_stems({"vocals_pct": 35, "bass_pct": 0}), "35% Vocals, No Bass")

    def test_choose_title_requires_non_empty_ending(self) -> None:
        with mock.patch("builtins.input", return_value="No Bass"):
            got = step5.choose_title("my_song", {"artist": "A", "title": "T"})
        self.assertEqual(got, "A - T (No Bass)")

    def test_choose_title_non_interactive_uses_suggested_or_default(self) -> None:
        got = step5.choose_title_with_options(
            "my_song",
            {"artist": "A", "title": "T", "vocals_pct": 35, "bass_pct": 0},
            interactive=False,
        )
        self.assertEqual(got, "A - T (35% Vocals, No Bass)")
        got2 = step5.choose_title_with_options("my_song", {"artist": "A", "title": "T"}, interactive=False)
        self.assertEqual(got2, "A - T (Karaoke)")

    def test_parse_args_defaults(self) -> None:
        args = step5.parse_args(["--slug", "song"])
        self.assertEqual(args.slug, "song")
        self.assertEqual(args.privacy, "unlisted")
        self.assertEqual(args.title, "")
        self.assertEqual(args.ending, "")
        self.assertFalse(args.yes)
        self.assertFalse(args.non_interactive)
        self.assertFalse(args.open_output_dir)

    def test_resolve_video_path_prefers_exact_then_newest_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            direct = out_dir / "song.mp4"
            alt_old = out_dir / "song.preview-old.mp4"
            alt_new = out_dir / "song.preview-new.mp4"
            alt_old.write_bytes(b"a")
            alt_new.write_bytes(b"b")
            os.utime(alt_old, (1000, 1000))
            os.utime(alt_new, (2000, 2000))

            with mock.patch.object(step5, "OUT_DIR", out_dir):
                self.assertEqual(step5._resolve_video_path("song"), alt_new)
                direct.write_bytes(b"direct")
                self.assertEqual(step5._resolve_video_path("song"), direct)

    def test_upload_video_returns_video_id(self) -> None:
        class Status:
            def __init__(self, p):
                self._p = p

            def progress(self):
                return self._p

        class Req:
            def __init__(self):
                self.i = 0

            def next_chunk(self):
                self.i += 1
                if self.i == 1:
                    return Status(0.5), None
                return None, {"id": "vid123"}

        class Videos:
            def insert(self, **kwargs):  # type: ignore[no-untyped-def]
                self.kwargs = kwargs
                return Req()

        class Youtube:
            def videos(self):
                return Videos()

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x.mp4"
            p.write_bytes(b"x")
            video_id = step5.upload_video(
                Youtube(),
                p,
                "Title",
                "Desc",
                ["t1"],
                category_id="10",
                privacy="unlisted",
            )
        self.assertEqual(video_id, "vid123")

    def test_set_thumbnail_calls_api(self) -> None:
        class ThumbsReq:
            def __init__(self):
                self.executed = False

            def execute(self):
                self.executed = True
                return {}

        class Thumbs:
            def __init__(self):
                self.req = ThumbsReq()

            def set(self, **kwargs):  # type: ignore[no-untyped-def]
                self.kwargs = kwargs
                return self.req

        class Youtube:
            def __init__(self):
                self._thumbs = Thumbs()

            def thumbnails(self):
                return self._thumbs

        yt = Youtube()
        with tempfile.TemporaryDirectory() as td:
            thumb = Path(td) / "thumb.jpg"
            thumb.write_bytes(b"x")
            step5.set_thumbnail(yt, "vid", thumb)
        self.assertTrue(yt._thumbs.req.executed)

    def test_main_aborts_when_user_declines_upload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "output"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "song.mp4").write_bytes(b"x")
            with (
                mock.patch.object(step5, "OUT_DIR", out_dir),
                mock.patch("scripts.step5_distribute.ask_yes_no", return_value=False),
                mock.patch("scripts.step5_distribute.load_meta_for_slug", return_value=None),
                mock.patch("scripts.step5_distribute.choose_title_with_options", return_value="Title"),
                mock.patch("scripts.step5_distribute.load_secrets_path") as secrets_mock,
            ):
                rc = step5.main(["--slug", "song"])
        self.assertEqual(rc, 0)
        secrets_mock.assert_not_called()

    def test_main_non_interactive_upload_writes_step5_meta_and_respects_privacy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "output"
            meta_dir = root / "meta"
            out_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            video_path = out_dir / "song.mp4"
            video_path.write_bytes(b"x")

            with (
                mock.patch.object(step5, "OUT_DIR", out_dir),
                mock.patch.object(step5, "META_DIR", meta_dir),
                mock.patch("scripts.step5_distribute.load_meta_for_slug", return_value={"artist": "A", "title": "T"}),
                mock.patch("scripts.step5_distribute.load_secrets_path", return_value=root / "client_secret.json"),
                mock.patch("scripts.step5_distribute.get_credentials", return_value=object()),
                mock.patch("scripts.step5_distribute.build", return_value=object()),
                mock.patch("scripts.step5_distribute.upload_video", return_value="vid123") as upload_mock,
                mock.patch("scripts.step5_distribute.extract_thumbnail", return_value=None),
                mock.patch("scripts.step5_distribute.set_thumbnail", return_value=None),
                mock.patch("scripts.step5_distribute.ask_yes_no") as ask_mock,
            ):
                rc = step5.main(["--slug", "song", "--privacy", "private", "--non-interactive", "--yes"])

            self.assertEqual(rc, 0)
            ask_mock.assert_not_called()
            call_args, kwargs = upload_mock.call_args
            self.assertGreaterEqual(len(call_args), 4)
            self.assertEqual(call_args[2], "A - T (Karaoke)")
            self.assertEqual(kwargs["privacy"], "private")
            written = json.loads((meta_dir / "song.step5.json").read_text(encoding="utf-8"))
            self.assertEqual(written["video_id"], "vid123")
            self.assertEqual(written["privacy"], "private")
            self.assertEqual(written["slug"], "song")

    def test_main_non_interactive_disables_browser_oauth_flow(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "output"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "song.mp4").write_bytes(b"x")

            with (
                mock.patch.object(step5, "OUT_DIR", out_dir),
                mock.patch("scripts.step5_distribute.load_meta_for_slug", return_value={"artist": "A", "title": "T"}),
                mock.patch("scripts.step5_distribute.load_secrets_path", return_value=root / "client_secret.json"),
                mock.patch("scripts.step5_distribute.get_credentials", return_value=object()) as creds_mock,
                mock.patch("scripts.step5_distribute.build", return_value=object()),
                mock.patch("scripts.step5_distribute.upload_video", return_value="vid123"),
                mock.patch("scripts.step5_distribute.extract_thumbnail", return_value=None),
                mock.patch("scripts.step5_distribute.set_thumbnail", return_value=None),
            ):
                rc = step5.main(["--slug", "song", "--non-interactive", "--yes"])

            self.assertEqual(rc, 0)
            self.assertEqual(creds_mock.call_count, 1)
            self.assertFalse(bool(creds_mock.call_args.kwargs.get("allow_oauth_login_flow")))

    def test_main_runs_sync_checks_when_enabled_and_writes_to_step5_meta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "output"
            meta_dir = root / "meta"
            out_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            video_path = out_dir / "song.mp4"
            video_path.write_bytes(b"x")

            pre_payload = {
                "slug": "song",
                "overall_passed": True,
                "elapsed_ms": 1200.0,
                "pre_upload": {
                    "scope": "pre_upload",
                    "passed": True,
                    "elapsed_ms": 1200.0,
                    "elapsed_sec": 1.2,
                    "checks": {
                        "audio_offset": {"status": "passed", "elapsed_sec": 0.6, "passed": True},
                        "visual_sync": {"status": "passed", "elapsed_sec": 0.6, "passed": True},
                    },
                },
            }
            post_payload = {
                "slug": "song",
                "overall_passed": True,
                "elapsed_ms": 2400.0,
                "post_upload": {
                    "scope": "post_upload",
                    "passed": True,
                    "elapsed_ms": 2400.0,
                    "elapsed_sec": 2.4,
                    "checks": {
                        "download": {"status": "passed", "elapsed_sec": 0.9, "passed": True},
                        "visual_sync": {"status": "passed", "elapsed_sec": 1.5, "passed": True},
                    },
                },
            }

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_SYNC_CHECKS_ENABLED": "1"}, clear=False),
                mock.patch.object(step5, "OUT_DIR", out_dir),
                mock.patch.object(step5, "META_DIR", meta_dir),
                mock.patch("scripts.step5_distribute.load_meta_for_slug", return_value={"artist": "A", "title": "T"}),
                mock.patch("scripts.step5_distribute.load_secrets_path", return_value=root / "client_secret.json"),
                mock.patch("scripts.step5_distribute.get_credentials", return_value=object()),
                mock.patch("scripts.step5_distribute.build", return_value=object()),
                mock.patch("scripts.step5_distribute.upload_video", return_value="vid123"),
                mock.patch("scripts.step5_distribute.extract_thumbnail", return_value=None),
                mock.patch("scripts.step5_distribute.set_thumbnail", return_value=None),
                mock.patch("scripts.step5_distribute.run_sync_quality_checks", side_effect=[pre_payload, post_payload]) as sync_mock,
            ):
                rc = step5.main(["--slug", "song", "--non-interactive", "--yes"])

            self.assertEqual(rc, 0)
            self.assertEqual(sync_mock.call_count, 2)
            written = json.loads((meta_dir / "song.step5.json").read_text(encoding="utf-8"))
            sync_meta = written.get("sync_checks") or {}
            self.assertIn("pre_upload", sync_meta)
            self.assertIn("post_upload", sync_meta)
            self.assertTrue(sync_meta.get("overall_passed"))

    def test_main_blocks_upload_when_sync_precheck_fails_and_blocking_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "output"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "song.mp4").write_bytes(b"x")

            pre_payload = {
                "slug": "song",
                "overall_passed": False,
                "elapsed_ms": 900.0,
                "pre_upload": {
                    "scope": "pre_upload",
                    "passed": False,
                    "elapsed_ms": 900.0,
                    "elapsed_sec": 0.9,
                    "checks": {"visual_sync": {"status": "failed", "passed": False, "elapsed_sec": 0.9}},
                },
            }

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_SYNC_CHECKS_ENABLED": "1",
                        "MIXTERIOSO_SYNC_CHECKS_BLOCK_ON_FAIL": "1",
                    },
                    clear=False,
                ),
                mock.patch.object(step5, "OUT_DIR", out_dir),
                mock.patch("scripts.step5_distribute.load_meta_for_slug", return_value={"artist": "A", "title": "T"}),
                mock.patch("scripts.step5_distribute.run_sync_quality_checks", return_value=pre_payload),
                mock.patch("scripts.step5_distribute.upload_video") as upload_mock,
            ):
                rc = step5.main(["--slug", "song", "--non-interactive", "--yes"])

            self.assertEqual(rc, 1)
            upload_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
