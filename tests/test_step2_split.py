import json
import threading
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from scripts.common import IOFlags, Paths
from scripts import step2_split as step2


class Step2SplitTests(unittest.TestCase):
    def test_pct_to_gain_handles_numeric_and_bad_values(self) -> None:
        self.assertEqual(step2._pct_to_gain(150), 1.5)
        self.assertEqual(step2._pct_to_gain("50"), 0.5)
        self.assertEqual(step2._pct_to_gain("bad"), 1.0)

    def test_normalize_stem_pct_clamps_and_defaults(self) -> None:
        self.assertEqual(step2._normalize_stem_pct(-50), 0.0)
        self.assertEqual(step2._normalize_stem_pct(180), 150.0)
        self.assertEqual(step2._normalize_stem_pct("bad"), 100.0)

    def test_is_demucs_runtime_unavailable_error_detects_backend_failure(self) -> None:
        self.assertTrue(
            step2._is_demucs_runtime_unavailable_error(
                "Couldn't find appropriate backend to handle uri /tmp/vocals.wav and format None."
            )
        )
        self.assertTrue(step2._is_demucs_runtime_unavailable_error("numpy is not available"))
        self.assertFalse(step2._is_demucs_runtime_unavailable_error("network timeout"))

    def test_is_vocals_only_adjustment(self) -> None:
        self.assertTrue(step2._is_vocals_only_adjustment(0, 100, 100, 100))
        self.assertFalse(step2._is_vocals_only_adjustment(0, 90, 100, 100))

    def test_fast_vocals_fallback_disallows_full_vocal_mute_and_aggressive_reduction(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT": "50",
                "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MAX_PCT": "100",
            },
            clear=False,
        ):
            self.assertFalse(step2._fast_vocals_fallback_allowed(0, 100, 100, 100))
            self.assertFalse(step2._fast_vocals_fallback_allowed(10, 100, 100, 100))
            self.assertTrue(step2._fast_vocals_fallback_allowed(80, 100, 100, 100))

    def test_effective_demucs_overlap_uses_fast_value_for_two_stem_vocals_only_mix(self) -> None:
        with (
            mock.patch.object(step2, "DEMUCS_OVERLAP", 0.10),
            mock.patch.object(step2, "DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP", 0.0),
            step2._temporary_demucs_two_stems(True),
        ):
            overlap = step2._effective_demucs_overlap(
                {"vocals": 20.0, "bass": 100.0, "drums": 100.0, "other": 100.0}
            )
        self.assertEqual(overlap, 0.0)

    def test_effective_demucs_overlap_keeps_default_for_non_vocals_only_mix(self) -> None:
        with (
            mock.patch.object(step2, "DEMUCS_OVERLAP", 0.10),
            mock.patch.object(step2, "DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP", 0.0),
            step2._temporary_demucs_two_stems(True),
        ):
            overlap = step2._effective_demucs_overlap(
                {"vocals": 20.0, "bass": 90.0, "drums": 100.0, "other": 100.0}
            )
        self.assertEqual(overlap, 0.10)

    def test_fast_vocals_fallback_allowed_honors_threshold(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT": "50",
                "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MAX_PCT": "90",
            },
            clear=False,
        ):
            self.assertFalse(step2._fast_vocals_fallback_allowed(20.0, 100.0, 100.0, 100.0))
            self.assertTrue(step2._fast_vocals_fallback_allowed(50.0, 100.0, 100.0, 100.0))
            self.assertTrue(step2._fast_vocals_fallback_allowed(90.0, 100.0, 100.0, 100.0))
            self.assertFalse(step2._fast_vocals_fallback_allowed(91.0, 100.0, 100.0, 100.0))
            self.assertFalse(step2._fast_vocals_fallback_allowed(50.0, 90.0, 100.0, 100.0))

    def test_temporary_demucs_two_stems_is_thread_local(self) -> None:
        barrier = threading.Barrier(2)
        observed: dict[str, list[str]] = {}

        def _run(name: str, enabled: bool) -> None:
            with step2._temporary_demucs_two_stems(enabled):
                barrier.wait(timeout=2)
                observed[name] = step2._required_stem_names()

        t_two = threading.Thread(target=_run, args=("two", True))
        t_four = threading.Thread(target=_run, args=("four", False))
        t_two.start()
        t_four.start()
        t_two.join(timeout=2)
        t_four.join(timeout=2)

        self.assertEqual(observed.get("two"), ["vocals", "no_vocals"])
        self.assertEqual(observed.get("four"), ["vocals", "bass", "drums", "other"])

    def test_normalize_worker_url_adds_default_separate_path(self) -> None:
        self.assertEqual(
            step2._normalize_worker_url("https://worker.placeholder.invalid"),
            "https://worker.placeholder.invalid/separate",
        )
        self.assertEqual(
            step2._normalize_worker_url("https://worker.placeholder.invalid/"),
            "https://worker.placeholder.invalid/separate",
        )
        self.assertEqual(
            step2._normalize_worker_url("https://worker.placeholder.invalid/v1/separate"),
            "https://worker.placeholder.invalid/v1/separate",
        )

    def test_gpu_worker_config_supports_karaoapi_aliases_for_fallback_and_hmac(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "MIXTERIOSO_GPU_WORKER_URL": "https://worker.placeholder.invalid",
                "MIXTERIOSO_DISABLE_LOCAL_SPLIT_FALLBACK": "1",
                "KARAOAPI_GPU_FALLBACK_TO_CPU": "1",
                "KARAOAPI_GPU_WORKER_HMAC_SECRET": "secret-from-alias",
                "KARAOAPI_GPU_WORKER_REQUIRE_HMAC": "1",
            },
            clear=False,
        ):
            cfg = step2._gpu_worker_config()
        self.assertEqual(cfg.url, "https://worker.placeholder.invalid/separate")
        self.assertTrue(cfg.fallback_to_cpu)
        self.assertEqual(cfg.hmac_secret, "secret-from-alias")
        self.assertTrue(cfg.require_hmac)

    def test_resolve_demucs_device_honors_explicit_env(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MIXTERIOSO_DEMUCS_DEVICE": "mps"}),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(False, True)),
            mock.patch("scripts.step2_split.sys.platform", "darwin"),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "mps")
        with (
            mock.patch.dict("os.environ", {"MIXTERIOSO_DEMUCS_DEVICE": "gpu"}),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(True, False)),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cuda")

    def test_resolve_demucs_device_auto_prefers_cuda_then_mps_then_cpu(self) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "MIXTERIOSO_DEMUCS_DEVICE": "auto",
                    "MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE": "0",
                },
            ),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(True, True)),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cuda")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "MIXTERIOSO_DEMUCS_DEVICE": "auto",
                    "MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE": "0",
                },
            ),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(False, True)),
            mock.patch("scripts.step2_split.sys.platform", "darwin"),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "mps")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "MIXTERIOSO_DEMUCS_DEVICE": "auto",
                    "MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE": "0",
                },
            ),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(False, False)),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cpu")

    def test_resolve_demucs_device_auto_skips_mps_on_non_macos(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MIXTERIOSO_DEMUCS_DEVICE": "auto"}, clear=True),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(False, True)),
            mock.patch("scripts.step2_split.sys.platform", "linux"),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cpu")

    def test_ensure_wav_from_audio_skips_when_output_is_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.mp3"
            out = Path(td) / "out.wav"
            src.write_bytes(b"a")
            out.write_bytes(b"b")
            now = time.time()
            src_ts = now - 60
            out_ts = now
            src.touch()
            out.touch()
            Path(src).chmod(0o644)
            Path(out).chmod(0o644)
            import os

            os.utime(src, (src_ts, src_ts))
            os.utime(out, (out_ts, out_ts))

            with mock.patch("scripts.step2_split.run_cmd") as run_cmd:
                step2._ensure_wav_from_audio(src, out, IOFlags(force=False))
            run_cmd.assert_not_called()

    def test_ensure_wav_from_audio_requires_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.mp3"
            out = Path(td) / "out.wav"
            src.write_bytes(b"a")
            with mock.patch("scripts.step2_split.have_exe", return_value=False):
                with self.assertRaises(RuntimeError):
                    step2._ensure_wav_from_audio(src, out, IOFlags(force=False))

    def test_ensure_wav_from_audio_runs_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.mp3"
            out = Path(td) / "out.wav"
            src.write_bytes(b"a")

            def fake_run_cmd(cmd, tag, dry_run, check=False, **kwargs):  # type: ignore[no-untyped-def]
                self.assertTrue(check or kwargs.get("check", False))
                self.assertIn("ffmpeg", cmd[0])
                self.assertIn("pcm_s16le", cmd)
                out.write_bytes(b"wav")

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._ensure_wav_from_audio(src, out, IOFlags(force=False))
            self.assertTrue(out.exists())

    def test_encode_mp3_from_wav_runs_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.wav"
            out = Path(td) / "out.mp3"
            src.write_bytes(b"a")

            def fake_run_cmd(cmd, tag, dry_run, check=False, **kwargs):  # type: ignore[no-untyped-def]
                self.assertTrue(check or kwargs.get("check", False))
                self.assertIn("libmp3lame", cmd)
                out.write_bytes(b"mp3")

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._encode_mp3_from_wav(src, out, IOFlags(force=False))
            self.assertTrue(out.exists())

    def test_ensure_demucs_stems_uses_resolved_device(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, **kwargs):  # type: ignore[no-untyped-def]
                captured["cmd"] = cmd
                stem_dir = paths.separated / "htdemucs" / slug
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split._resolve_demucs_device", return_value="cuda"),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._ensure_demucs_stems(paths, slug, src_mp3, IOFlags(force=False))

            cmd = captured["cmd"]
            self.assertIn("-d", cmd)
            self.assertEqual(cmd[cmd.index("-d") + 1], "cuda")

    def test_ensure_demucs_stems_uses_fast_overlap_for_two_stem_vocals_only_mix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_m4a = paths.mp3s / f"{slug}.m4a"
            src_m4a.parent.mkdir(parents=True, exist_ok=True)
            src_m4a.write_bytes(b"source")
            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, **kwargs):  # type: ignore[no-untyped-def]
                captured["cmd"] = cmd
                stem_dir = paths.separated / "htdemucs" / slug
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "no_vocals"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch.object(step2, "DEMUCS_OVERLAP", 0.10),
                mock.patch.object(step2, "DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP", 0.0),
                step2._temporary_demucs_two_stems(True),
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split._resolve_demucs_device", return_value="mps"),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._ensure_demucs_stems(
                    paths,
                    slug,
                    src_m4a,
                    IOFlags(force=False),
                    stem_profile={"vocals": 20.0, "bass": 100.0, "drums": 100.0, "other": 100.0},
                )

            cmd = captured["cmd"]
            self.assertIn("--overlap", cmd)
            self.assertEqual(cmd[cmd.index("--overlap") + 1], "0.0")

    def test_ensure_demucs_stems_canonicalizes_race_suffix_input_name(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "goo_goo_dolls_iris"
            src_audio = paths.mp3s / f"{slug}.race2.m4a"
            src_audio.parent.mkdir(parents=True, exist_ok=True)
            src_audio.write_bytes(b"source")
            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, **kwargs):  # type: ignore[no-untyped-def]
                captured["cmd"] = cmd
                stem_dir = paths.separated / "htdemucs" / slug
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
                mock.patch("scripts.step2_split._resolve_demucs_device", return_value="cpu"),
            ):
                step2._ensure_demucs_stems(paths, slug, src_audio, IOFlags(force=False))

            demucs_input = Path(str(captured["cmd"][-1]))
            self.assertEqual(demucs_input.stem, slug)
            self.assertEqual(demucs_input.suffix, ".m4a")
            self.assertNotIn(".race", demucs_input.name)

    def test_mix_stems_to_wav_builds_filter_graph_from_percentages(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "mix.wav"
            stems = [root / f"{name}.wav" for name in ("vocals", "bass", "drums", "other")]
            for stem in stems:
                stem.write_bytes(b"x" * 4096)

            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, check=False, **kwargs):  # type: ignore[no-untyped-def]
                self.assertTrue(check or kwargs.get("check", False))
                captured["cmd"] = cmd
                out.write_bytes(b"wav")

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._mix_stems_to_wav(
                    vocals_wav=stems[0],
                    bass_wav=stems[1],
                    drums_wav=stems[2],
                    other_wav=stems[3],
                    vocals_pct=80,
                    bass_pct=120,
                    drums_pct=100,
                    other_pct=50,
                    out_wav=out,
                    flags=IOFlags(force=False),
                )

            cmd = captured["cmd"]
            fc = cmd[cmd.index("-filter_complex") + 1]
            self.assertIn("volume=0.8", fc)
            self.assertIn("volume=1.2", fc)
            self.assertIn("volume=0.5", fc)
            self.assertTrue(out.exists())

    def test_mix_stems_to_wav_two_stems_mutes_vocals_when_zero_percent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "mix.wav"
            vocals = root / "vocals.wav"
            no_vocals = root / "no_vocals.wav"
            vocals.write_bytes(b"x" * 4096)
            no_vocals.write_bytes(b"x" * 4096)

            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, check=False, **kwargs):  # type: ignore[no-untyped-def]
                self.assertTrue(check or kwargs.get("check", False))
                captured["cmd"] = cmd
                out.write_bytes(b"wav")

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
            ):
                step2._mix_stems_to_wav(
                    vocals_wav=vocals,
                    bass_wav=no_vocals,
                    drums_wav=no_vocals,
                    other_wav=no_vocals,
                    vocals_pct=0,
                    bass_pct=100,
                    drums_pct=100,
                    other_pct=100,
                    out_wav=out,
                    flags=IOFlags(force=False),
                )

            cmd = captured["cmd"]
            fc = cmd[cmd.index("-filter_complex") + 1]
            self.assertIn("[0:a]volume=0.0[v];", fc)
            self.assertIn("[1:a]volume=1.0[i];", fc)
            self.assertIn("amix=inputs=2", fc)
            self.assertTrue(out.exists())

    def test_step2_split_full_mode_copies_mp3_and_writes_mix_meta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            def fake_ensure_wav(src_audio, out_wav, flags):  # type: ignore[no-untyped-def]
                self.assertEqual(src_audio, paths.mixes / f"{slug}.mp3")
                out_wav.write_bytes(b"wav")

            with mock.patch("scripts.step2_split._ensure_wav_from_audio", side_effect=fake_ensure_wav):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="full",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=False),
                )

            out_mp3 = paths.mixes / f"{slug}.mp3"
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(out_mp3.read_bytes(), b"source")
            self.assertEqual(meta["mode"], "full")

    def test_step2_split_invalid_mix_mode_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            with self.assertRaises(ValueError):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="nope",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=False),
                )

    def test_step2_split_switches_to_stems_when_levels_change(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            def fake_mix(**kwargs):  # type: ignore[no-untyped-def]
                kwargs["out_wav"].write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0"}, clear=False),
                mock.patch("scripts.step2_split._ensure_demucs_stems", return_value=stem_dir) as ensure_stems,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="full",
                    vocals=80,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            ensure_stems.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "stems")
            self.assertEqual(meta["levels_percent"]["vocals"], 80.0)
            self.assertIn("demucs_device", meta)

    def test_step2_split_uses_vocals_fallback_when_demucs_backend_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            fallback_error = RuntimeError(
                "Couldn't find appropriate backend to handle uri /tmp/vocals.wav and format None."
            )

            def fake_fallback_mix(*, src_audio, vocals_pct, out_wav, flags):  # type: ignore[no-untyped-def]
                self.assertEqual(src_audio, src_mp3)
                self.assertEqual(vocals_pct, 0)
                out_wav.write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                self.assertTrue(src_wav.exists())
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0"}, clear=False),
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_singleflight",
                    side_effect=fallback_error,
                ),
                mock.patch(
                    "scripts.step2_split._mix_vocals_only_fallback_to_wav",
                    side_effect=fake_fallback_mix,
                ) as fallback_mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=0,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            fallback_mix_mock.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "fallback_vocals_only")
            self.assertEqual(meta["demucs_backend"], "fallback_vocals_center_cancel")

    def test_step2_split_fast_vocals_first_uses_demucs_for_full_vocal_mute(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            (stem_dir / "vocals.wav").write_bytes(b"vocals")
            (stem_dir / "no_vocals.wav").write_bytes(b"inst")

            def fake_mix_stems(  # type: ignore[no-untyped-def]
                *,
                vocals_wav,
                bass_wav,
                drums_wav,
                other_wav,
                vocals_pct,
                bass_pct,
                drums_pct,
                other_pct,
                out_wav,
                flags,
            ):
                self.assertEqual(vocals_wav, stem_dir / "vocals.wav")
                self.assertEqual(bass_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(drums_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(other_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(vocals_pct, 0)
                self.assertEqual((bass_pct, drums_pct, other_pct), (100, 100, 100))
                out_wav.write_bytes(b"wav")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "1",
                        "MIXTERIOSO_AUTO_TWO_STEMS_FOR_VOCALS_ONLY": "1",
                        "MIXTERIOSO_STEP2_SKIP_STEMS_MIX_MP3": "1",
                    },
                    clear=False,
                ),
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_singleflight",
                    return_value=(stem_dir, "local_demucs"),
                ) as ensure_stems,
                mock.patch("scripts.step2_split._mix_vocals_only_fallback_to_wav") as fallback_mix_mock,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix_stems) as mix_mock,
                mock.patch("scripts.step2_split._start_prepared_render_audio_async") as prep_render_audio,
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=0,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            ensure_stems.assert_called_once()
            fallback_mix_mock.assert_not_called()
            mix_mock.assert_called_once()
            prep_render_audio.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "stems")
            self.assertEqual(meta["demucs_backend"], "local_demucs")
            self.assertEqual(float(meta["levels_percent"]["vocals"]), 0.0)

    def test_step2_split_fast_vocals_first_uses_demucs_for_partial_vocals_mix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            (stem_dir / "vocals.wav").write_bytes(b"vocals")
            (stem_dir / "no_vocals.wav").write_bytes(b"inst")

            def fake_mix_stems(  # type: ignore[no-untyped-def]
                *,
                vocals_wav,
                bass_wav,
                drums_wav,
                other_wav,
                vocals_pct,
                bass_pct,
                drums_pct,
                other_pct,
                out_wav,
                flags,
            ):
                self.assertEqual(vocals_wav, stem_dir / "vocals.wav")
                self.assertEqual(bass_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(drums_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(other_wav, stem_dir / "no_vocals.wav")
                self.assertEqual(vocals_pct, 20)
                self.assertEqual((bass_pct, drums_pct, other_pct), (100, 100, 100))
                self.assertTrue(flags.force)
                out_wav.write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                self.assertTrue(src_wav.exists())
                self.assertTrue(flags.force)
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "1",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT": "50",
                        "MIXTERIOSO_AUTO_TWO_STEMS_FOR_VOCALS_ONLY": "1",
                    },
                    clear=False,
                ),
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_singleflight",
                    return_value=(stem_dir, "local_demucs"),
                ) as ensure_stems,
                mock.patch("scripts.step2_split._mix_vocals_only_fallback_to_wav") as fallback_mix_mock,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix_stems) as mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=20,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            ensure_stems.assert_called_once()
            fallback_mix_mock.assert_not_called()
            mix_mock.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "stems")
            self.assertEqual(meta["demucs_backend"], "local_demucs")
            self.assertEqual(meta["levels_percent"]["vocals"], 20.0)

    def test_step2_split_fast_vocals_first_skips_demucs_for_partial_vocals_mix_with_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            def fake_fallback_mix(*, src_audio, vocals_pct, out_wav, flags):  # type: ignore[no-untyped-def]
                self.assertEqual(src_audio, src_mp3)
                self.assertEqual(vocals_pct, 80)
                self.assertTrue(flags.force)
                out_wav.write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                self.assertTrue(src_wav.exists())
                self.assertTrue(flags.force)
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "1",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT": "50",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MAX_PCT": "100",
                    },
                    clear=False,
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems_singleflight") as ensure_stems,
                mock.patch(
                    "scripts.step2_split._mix_vocals_only_fallback_to_wav",
                    side_effect=fake_fallback_mix,
                ) as fallback_mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=80,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            ensure_stems.assert_not_called()
            fallback_mix_mock.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "fallback_vocals_only")
            self.assertEqual(meta["demucs_backend"], "fallback_vocals_center_cancel_fast_first")
            self.assertEqual(float(meta["levels_percent"]["vocals"]), 80.0)

    def test_step2_split_fast_vocals_first_runtime_skip_mp3_encode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            def fake_fallback_mix(*, out_wav, **_kwargs):  # type: ignore[no-untyped-def]
                out_wav.write_bytes(b"wav")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "1",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT": "50",
                        "MIXTERIOSO_STEP2_SKIP_STEMS_MIX_MP3": "1",
                    },
                    clear=False,
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems_singleflight") as ensure_stems,
                mock.patch(
                    "scripts.step2_split._mix_vocals_only_fallback_to_wav",
                    side_effect=fake_fallback_mix,
                ),
                mock.patch("scripts.step2_split._encode_mp3_from_wav") as enc_mock,
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=80,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            ensure_stems.assert_not_called()
            enc_mock.assert_not_called()
            self.assertFalse((paths.mixes / f"{slug}.mp3").exists())
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertTrue(bool(meta.get("mp3_skipped")))

    def test_apply_vocals_only_fallback_mix_reuses_existing_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            out_wav = paths.mixes / f"{slug}.wav"
            out_mp3 = paths.mixes / f"{slug}.mp3"
            out_wav.parent.mkdir(parents=True, exist_ok=True)
            out_wav.write_bytes(b"wav")
            out_mp3.write_bytes(b"mp3")

            (paths.mixes / f"{slug}.mix.json").write_text(
                json.dumps(
                    {
                        "mode": "fallback_vocals_only",
                        "src": str(src_mp3),
                        "mix_mp3": str(out_mp3),
                        "mix_wav": str(out_wav),
                        "mp3_skipped": False,
                        "levels_percent": {
                            "vocals": 20.0,
                            "bass": 100.0,
                            "drums": 100.0,
                            "other": 100.0,
                        },
                        "demucs_backend": "fallback_vocals_center_cancel",
                        "demucs_error": "",
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch("scripts.step2_split._mix_vocals_only_fallback_to_wav") as mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav") as encode_mock,
            ):
                step2._apply_vocals_only_fallback_mix(
                    paths=paths,
                    slug=slug,
                    src_audio=src_mp3,
                    out_wav=out_wav,
                    out_mp3=out_mp3,
                    vocals=20.0,
                    bass=100.0,
                    drums=100.0,
                    other=100.0,
                    flags=IOFlags(force=False),
                    reason_text="Demucs runtime unavailable",
                    demucs_backend_label="fallback_vocals_center_cancel",
                )

            mix_mock.assert_not_called()
            encode_mock.assert_not_called()

    def test_step2_split_uses_vocals_fallback_when_demucs_separation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            fallback_error = RuntimeError("demucs separation failed: DEMUCS failed rc=1")

            def fake_fallback_mix(*, src_audio, vocals_pct, out_wav, flags):  # type: ignore[no-untyped-def]
                self.assertEqual(src_audio, src_mp3)
                self.assertEqual(vocals_pct, 20)
                out_wav.write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                self.assertTrue(src_wav.exists())
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0"}, clear=False),
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_singleflight",
                    side_effect=fallback_error,
                ),
                mock.patch(
                    "scripts.step2_split._mix_vocals_only_fallback_to_wav",
                    side_effect=fake_fallback_mix,
                ) as fallback_mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=20,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            fallback_mix_mock.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "fallback_vocals_only")
            self.assertEqual(meta["demucs_backend"], "fallback_vocals_center_cancel")

    def test_step2_split_raises_when_demucs_unavailable_with_multi_stem_adjustments(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            fallback_error = RuntimeError(
                "Couldn't find appropriate backend to handle uri /tmp/vocals.wav and format None."
            )

            with mock.patch(
                "scripts.step2_split._ensure_demucs_stems_singleflight",
                side_effect=fallback_error,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2.step2_split(
                        paths,
                        slug=slug,
                        mix_mode="stems",
                        vocals=100,
                        bass=80,
                        drums=100,
                        other=100,
                        flags=IOFlags(force=True),
                    )

            self.assertIn("Could not process audio stems for requested bass/drums/other levels", str(cm.exception))

    def test_step2_split_restores_global_cache_when_force_and_demucs_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")

            stem_profile = {
                "vocals": 100.0,
                "bass": 80.0,
                "drums": 100.0,
                "other": 100.0,
            }
            cache_dir = step2._resolve_global_stem_cache_dir(
                paths,
                src_mp3,
                stem_profile=stem_profile,
                model_version="htdemucs",
            )
            cache_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (cache_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            fallback_error = RuntimeError(
                "Couldn't find appropriate backend to handle uri /tmp/vocals.wav and format None."
            )

            def fake_mix(*, out_wav, **_kwargs):  # type: ignore[no-untyped-def]
                out_wav.write_bytes(b"wav")

            def fake_encode(_src_wav, out_mp3, _flags):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_singleflight",
                    side_effect=fallback_error,
                ),
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
                mock.patch("scripts.step2_split._resolve_demucs_device", return_value="cpu"),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=80,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["mode"], "stems")
            self.assertEqual(meta["demucs_backend"], "global_stem_cache_force_fallback")
            self.assertFalse((paths.mixes / f"{slug}.mp3").exists())
            self.assertTrue((paths.mixes / f"{slug}.wav").exists())
            self.assertTrue(bool(meta.get("mp3_skipped")))

    def test_step2_split_raises_when_vocals_boost_requested_without_demucs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            fallback_error = RuntimeError(
                "Couldn't find appropriate backend to handle uri /tmp/vocals.wav and format None."
            )

            with mock.patch(
                "scripts.step2_split._ensure_demucs_stems_singleflight",
                side_effect=fallback_error,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2.step2_split(
                        paths,
                        slug=slug,
                        mix_mode="stems",
                        vocals=130,
                        bass=100,
                        drums=100,
                        other=100,
                        flags=IOFlags(force=True),
                    )

            self.assertIn("vocals boost requires Demucs runtime", str(cm.exception))

    def test_step2_split_reuses_existing_stems_wav_when_meta_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            out_wav = paths.mixes / f"{slug}.wav"
            out_wav.parent.mkdir(parents=True, exist_ok=True)
            out_wav.write_bytes(b"wav")
            (paths.mixes / f"{slug}.mix.json").write_text(
                json.dumps(
                    {
                        "mode": "stems",
                        "levels_percent": {"vocals": 100, "bass": 100, "drums": 100, "other": 100},
                    }
                ),
                encoding="utf-8",
            )

            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0"}, clear=False),
                mock.patch("scripts.step2_split._ensure_demucs_stems", return_value=stem_dir),
                mock.patch("scripts.step2_split._mix_stems_to_wav") as mix_mock,
                mock.patch("scripts.step2_split._encode_mp3_from_wav") as enc_mock,
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=False),
                )

            mix_mock.assert_not_called()
            enc_mock.assert_not_called()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertTrue(bool(meta.get("mp3_skipped")))

    def test_resolve_source_audio_prefers_step1_meta_audio_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_native = paths.mp3s / f"{slug}.m4a"
            src_native.parent.mkdir(parents=True, exist_ok=True)
            src_native.write_bytes(b"native")
            paths.meta.mkdir(parents=True, exist_ok=True)
            (paths.meta / f"{slug}.step1.json").write_text(
                json.dumps({"audio_path": str(src_native), "mp3": str(paths.mp3s / f"{slug}.mp3")}),
                encoding="utf-8",
            )

            got = step2._resolve_source_audio(paths, slug)
            self.assertEqual(got, src_native)

    def test_step2_split_full_mode_non_mp3_skips_mix_mp3_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_native = paths.mp3s / f"{slug}.m4a"
            src_native.parent.mkdir(parents=True, exist_ok=True)
            src_native.write_bytes(b"native")
            paths.meta.mkdir(parents=True, exist_ok=True)
            (paths.meta / f"{slug}.step1.json").write_text(
                json.dumps({"audio_path": str(src_native)}),
                encoding="utf-8",
            )

            def fake_ensure_wav(src_audio, out_wav, flags):  # type: ignore[no-untyped-def]
                self.assertEqual(src_audio, src_native)
                out_wav.write_bytes(b"wav")

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_STEP2_SKIP_FULL_MIX_MP3_WHEN_SOURCE_NOT_MP3": "1"}, clear=False),
                mock.patch("scripts.step2_split._ensure_wav_from_audio", side_effect=fake_ensure_wav),
                mock.patch("scripts.step2_split._encode_mp3_from_wav") as enc_mock,
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="full",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=False),
                )

            enc_mock.assert_not_called()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertTrue(meta["mp3_skipped"])
            self.assertEqual(meta["src_ext"], ".m4a")

    def test_step2_split_raises_when_source_mp3_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = Paths(root=Path(td))
            with self.assertRaises(RuntimeError):
                step2.step2_split(
                    paths,
                    slug="missing",
                    mix_mode="full",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=False),
                )

    def test_resolve_demucs_device_auto_prefers_cuda(self) -> None:
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(True, False)),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cuda")

    def test_resolve_demucs_device_explicit_mps_falls_back_to_cpu_when_unavailable(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MIXTERIOSO_DEMUCS_DEVICE": "mps"}, clear=True),
            mock.patch("scripts.step2_split._probe_torch_device_support", return_value=(False, False)),
        ):
            self.assertEqual(step2._resolve_demucs_device(), "cpu")

    def test_ensure_demucs_stems_uses_resolved_device(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = Paths(root=Path(td))
            slug = "song"
            src = paths.mp3s / f"{slug}.mp3"
            src.parent.mkdir(parents=True, exist_ok=True)
            src.write_bytes(b"source")
            captured = {}

            def fake_run_cmd(cmd, tag, dry_run, **kwargs):  # type: ignore[no-untyped-def]
                captured["cmd"] = cmd
                stem_dir = paths.separated / "htdemucs" / slug
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch("scripts.step2_split.have_exe", return_value=True),
                mock.patch("scripts.step2_split.run_cmd", side_effect=fake_run_cmd),
                mock.patch("scripts.step2_split._resolve_demucs_device", return_value="cuda"),
            ):
                step2._ensure_demucs_stems(paths, slug, src, IOFlags(force=False))

            cmd = captured["cmd"]
            self.assertIn("-d", cmd)
            self.assertEqual(cmd[cmd.index("-d") + 1], "cuda")

    def test_step2_split_uses_gpu_worker_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            def fake_mix(**kwargs):  # type: ignore[no-untyped-def]
                kwargs["out_wav"].write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_GPU_WORKER_URL": "https://gpu-worker.placeholder.invalid",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0",
                    },
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems_via_worker", return_value=stem_dir) as worker_mock,
                mock.patch("scripts.step2_split._ensure_demucs_stems") as local_mock,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            worker_mock.assert_called_once()
            local_mock.assert_not_called()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["demucs_backend"], "gpu_worker")

    def test_ensure_demucs_stems_uses_global_cache_before_demucs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")

            cache_dir = step2._resolve_global_stem_cache_dir(paths, src_mp3)
            cache_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (cache_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", True),
                mock.patch("scripts.step2_split.have_exe", return_value=False),
            ):
                stem_dir = step2._ensure_demucs_stems(paths, slug, src_mp3, IOFlags(force=False))

            self.assertEqual(stem_dir, paths.separated / "htdemucs" / slug)
            for name in ("vocals", "bass", "drums", "other"):
                self.assertTrue((stem_dir / f"{name}.wav").exists())
            meta = json.loads((paths.meta / f"{slug}.step2_stems.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["backend"], "global_stem_cache")

    def test_ensure_demucs_stems_via_worker_uses_global_cache_before_worker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")

            cache_dir = step2._resolve_global_stem_cache_dir(paths, src_mp3)
            cache_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (cache_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=0)
            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", True),
                mock.patch("scripts.step2_split._request_gpu_worker") as worker_req,
            ):
                stem_dir = step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            worker_req.assert_not_called()
            self.assertEqual(stem_dir, paths.separated / "htdemucs" / slug)

    def test_prune_global_stem_cache_keeps_newest_within_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            cache_root = step2._resolve_global_stem_cache_root(paths)
            now = time.time()

            def write_cache_entry(name: str, published_at: float) -> Path:
                entry = cache_root / name
                entry.mkdir(parents=True, exist_ok=True)
                for stem in ("vocals", "bass", "drums", "other"):
                    (entry / f"{stem}.wav").write_bytes(b"x" * 4096)
                (entry / ".cache_meta.json").write_text(
                    json.dumps({"published_at_epoch": published_at}),
                    encoding="utf-8",
                )
                return entry

            old_dir = write_cache_entry("old", now - 300.0)
            mid_dir = write_cache_entry("mid", now - 200.0)
            new_dir = write_cache_entry("new", now - 100.0)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_PRUNE_ENABLED", True),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS", 0.0),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_MAX_ENTRIES", 2),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_MAX_AGE_SECS", 3600.0),
                mock.patch.object(step2, "_GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO", 0.0),
            ):
                step2._prune_global_stem_cache(paths=paths, preserve_dirs=[])

            self.assertFalse(old_dir.exists())
            self.assertTrue(mid_dir.exists())
            self.assertTrue(new_dir.exists())

    def test_prune_global_stem_cache_removes_entries_older_than_max_age(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            cache_root = step2._resolve_global_stem_cache_root(paths)
            now = time.time()

            def write_cache_entry(name: str, published_at: float) -> Path:
                entry = cache_root / name
                entry.mkdir(parents=True, exist_ok=True)
                for stem in ("vocals", "bass", "drums", "other"):
                    (entry / f"{stem}.wav").write_bytes(b"x" * 4096)
                (entry / ".cache_meta.json").write_text(
                    json.dumps({"published_at_epoch": published_at}),
                    encoding="utf-8",
                )
                return entry

            old_dir = write_cache_entry("old", now - 7200.0)
            fresh_dir = write_cache_entry("fresh", now - 10.0)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_PRUNE_ENABLED", True),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS", 0.0),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_MAX_ENTRIES", 50),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_MAX_AGE_SECS", 60.0),
                mock.patch.object(step2, "_GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO", 0.0),
            ):
                step2._prune_global_stem_cache(paths=paths, preserve_dirs=[])

            self.assertFalse(old_dir.exists())
            self.assertTrue(fresh_dir.exists())

    def test_ensure_demucs_stems_via_worker_opens_circuit_after_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=0)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_ENABLED", True),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_FAIL_THRESHOLD", 1),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_COOLDOWN_SECS", 120.0),
                mock.patch("scripts.step2_split._request_gpu_worker", side_effect=RuntimeError("worker boom")),
            ):
                with self.assertRaises(RuntimeError):
                    step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_ENABLED", True),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_FAIL_THRESHOLD", 1),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_COOLDOWN_SECS", 120.0),
                mock.patch("scripts.step2_split._request_gpu_worker") as worker_req,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            worker_req.assert_not_called()
            self.assertIn("circuit open", str(cm.exception).lower())

    def test_ensure_demucs_stems_via_worker_stops_when_circuit_opens_mid_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=2)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_ENABLED", True),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_FAIL_THRESHOLD", 1),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_COOLDOWN_SECS", 120.0),
                mock.patch.object(step2, "GPU_WORKER_RETRY_BACKOFF_SECS", 0.0),
                mock.patch("scripts.step2_split._request_gpu_worker", side_effect=RuntimeError("worker boom")) as worker_req,
                mock.patch("scripts.step2_split.time.sleep") as sleep_mock,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            self.assertIn("failed after", str(cm.exception).lower())
            self.assertEqual(worker_req.call_count, 1)
            sleep_mock.assert_not_called()

    def test_ensure_demucs_stems_via_worker_stops_when_key_circuit_opens_mid_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=2)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_ENABLED", True),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_FAIL_THRESHOLD", 1),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_COOLDOWN_SECS", 120.0),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_MAX_ENTRIES", 100),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_MAX_AGE_SECS", 3600.0),
                mock.patch.object(step2, "GPU_WORKER_RETRY_BACKOFF_SECS", 0.0),
                mock.patch("scripts.step2_split._request_gpu_worker", side_effect=RuntimeError("worker boom")) as worker_req,
                mock.patch("scripts.step2_split.time.sleep") as sleep_mock,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            self.assertIn("failed after", str(cm.exception).lower())
            self.assertEqual(worker_req.call_count, 1)
            sleep_mock.assert_not_called()

    def test_gpu_worker_key_circuit_clears_after_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            key = "unit-key"
            with (
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_ENABLED", True),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_FAIL_THRESHOLD", 2),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_COOLDOWN_SECS", 120.0),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_MAX_ENTRIES", 100),
                mock.patch.object(step2, "GPU_WORKER_KEY_CIRCUIT_MAX_AGE_SECS", 3600.0),
            ):
                step2._mark_gpu_worker_key_failure(paths, key=key, error="first")
                open_after_first, _ = step2._gpu_worker_key_circuit_status(paths, key=key)
                step2._mark_gpu_worker_key_failure(paths, key=key, error="second")
                open_after_second, _ = step2._gpu_worker_key_circuit_status(paths, key=key)
                step2._mark_gpu_worker_key_success(paths, key=key)
                open_after_success, _ = step2._gpu_worker_key_circuit_status(paths, key=key)

            self.assertFalse(open_after_first)
            self.assertTrue(open_after_second)
            self.assertFalse(open_after_success)
            payload = json.loads((paths.meta / "gpu_worker_key_circuit.json").read_text(encoding="utf-8"))
            self.assertNotIn(key, payload.get("entries", {}))

    def test_ensure_demucs_stems_via_worker_applies_retry_backoff_between_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=1)

            with (
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_CIRCUIT_ENABLED", False),
                mock.patch.object(step2, "GPU_WORKER_RETRY_BACKOFF_SECS", 0.25),
                mock.patch.object(step2, "GPU_WORKER_RETRY_BACKOFF_MAX_SECS", 0.25),
                mock.patch(
                    "scripts.step2_split._request_gpu_worker",
                    side_effect=[RuntimeError("worker boom"), {"ok": True, "status": "succeeded"}],
                ) as worker_req,
                mock.patch("scripts.step2_split._materialize_worker_stems", return_value=stem_dir),
                mock.patch("scripts.step2_split.time.sleep") as sleep_mock,
            ):
                got = step2._ensure_demucs_stems_via_worker(paths, slug, src_mp3, IOFlags(force=False), cfg)

            self.assertEqual(got, stem_dir)
            self.assertEqual(worker_req.call_count, 2)
            sleep_mock.assert_called_once()
            self.assertAlmostEqual(float(sleep_mock.call_args.args[0]), 0.25, places=2)

    def test_materialize_worker_stems_falls_back_to_uris_when_stems_dir_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)

            payload = {
                "ok": True,
                "status": "succeeded",
                # Worker can echo caller-provided output_dir path here.
                "stems_dir": str(stem_dir),
                "stems_uris": {
                    "vocals": "gs://bucket/stems/vocals.wav",
                    "no_vocals": "gs://bucket/stems/no_vocals.wav",
                },
                "stems": {
                    "vocals_path": str(stem_dir / "vocals.wav"),
                    "no_vocals_path": str(stem_dir / "no_vocals.wav"),
                },
            }

            def fake_download(url, out_path, timeout_secs):  # type: ignore[no-untyped-def]
                _ = (url, timeout_secs)
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                Path(out_path).write_bytes(b"x" * 4096)

            with (
                step2._temporary_demucs_two_stems(True),
                mock.patch("scripts.step2_split._download_worker_stem", side_effect=fake_download) as dl_mock,
            ):
                got = step2._materialize_worker_stems(
                    paths=paths,
                    slug=slug,
                    payload=payload,
                    timeout_secs=30.0,
                )

            self.assertEqual(got, stem_dir)
            self.assertEqual(dl_mock.call_count, 2)
            self.assertTrue((stem_dir / "vocals.wav").exists())
            self.assertTrue((stem_dir / "no_vocals.wav").exists())

    def test_step2_split_falls_back_to_local_demucs_when_worker_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            def fake_mix(**kwargs):  # type: ignore[no-untyped-def]
                kwargs["out_wav"].write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_GPU_WORKER_URL": "https://gpu-worker.placeholder.invalid",
                        "MIXTERIOSO_GPU_FALLBACK_TO_CPU": "1",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0",
                    },
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems_via_worker", side_effect=RuntimeError("timeout")),
                mock.patch("scripts.step2_split._ensure_demucs_stems", return_value=stem_dir) as local_mock,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            local_mock.assert_called_once()
            meta = json.loads((paths.mixes / f"{slug}.mix.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["demucs_backend"], "local_demucs_fallback")

    def test_step2_split_disables_two_stems_even_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            observed_required_stems: list[str] = []

            def fake_ensure(*args, **kwargs):  # type: ignore[no-untyped-def]
                _ = (args, kwargs)
                observed_required_stems[:] = list(step2._required_stem_names())
                return stem_dir, "local_demucs"

            def fake_mix(**kwargs):  # type: ignore[no-untyped-def]
                kwargs["out_wav"].write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                _ = (src_wav, flags)
                out_mp3.write_bytes(b"mp3")

            with (
                step2._temporary_demucs_two_stems(True),
                mock.patch.dict("os.environ", {"MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0"}, clear=False),
                mock.patch("scripts.step2_split._ensure_demucs_stems_singleflight", side_effect=fake_ensure),
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            self.assertEqual(observed_required_stems, ["vocals", "no_vocals"])

    def test_step2_split_raises_when_worker_fails_and_cpu_fallback_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_GPU_WORKER_URL": "https://gpu-worker.placeholder.invalid",
                        "MIXTERIOSO_GPU_FALLBACK_TO_CPU": "0",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0",
                    },
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems_via_worker", side_effect=RuntimeError("boom")),
                mock.patch("scripts.step2_split._ensure_demucs_stems") as local_mock,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step2.step2_split(
                        paths,
                        slug=slug,
                        mix_mode="stems",
                        vocals=100,
                        bass=100,
                        drums=100,
                        other=100,
                        flags=IOFlags(force=False),
                    )

            self.assertIn("CPU fallback is disabled", str(cm.exception))
            local_mock.assert_not_called()

    def test_step2_split_forces_cpu_fallback_for_worker_auth_failure_even_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            src_mp3 = paths.mp3s / f"{slug}.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source")
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            def fake_mix(**kwargs):  # type: ignore[no-untyped-def]
                kwargs["out_wav"].write_bytes(b"wav")

            def fake_encode(src_wav, out_mp3, flags):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_GPU_WORKER_URL": "https://gpu-worker.placeholder.invalid",
                        "MIXTERIOSO_GPU_FALLBACK_TO_CPU": "0",
                        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST": "0",
                    },
                ),
                mock.patch(
                    "scripts.step2_split._ensure_demucs_stems_via_worker",
                    side_effect=RuntimeError(
                        'worker HTTP 401: {"detail":"hmac required but worker secret is not configured"}'
                    ),
                ),
                mock.patch("scripts.step2_split._ensure_demucs_stems", return_value=stem_dir) as local_mock,
                mock.patch("scripts.step2_split._mix_stems_to_wav", side_effect=fake_mix),
                mock.patch("scripts.step2_split._encode_mp3_from_wav", side_effect=fake_encode),
            ):
                step2.step2_split(
                    paths,
                    slug=slug,
                    mix_mode="stems",
                    vocals=100,
                    bass=100,
                    drums=100,
                    other=100,
                    flags=IOFlags(force=True),
                )

            local_mock.assert_called_once()

    def test_materialize_worker_stems_downloads_uris_when_worker_returns_local_stem_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            slug = "song"
            stem_dir = paths.separated / "htdemucs" / slug
            stem_dir.mkdir(parents=True, exist_ok=True)
            worker_dir = root / "worker"
            worker_dir.mkdir(parents=True, exist_ok=True)

            with step2._temporary_demucs_two_stems(False):
                stems_uris: dict[str, str] = {}
                for name in step2._required_stem_names():
                    src = worker_dir / f"{name}.wav"
                    src.write_bytes(b"x" * 4096)
                    stems_uris[name] = f"file://{src}"

                got = step2._materialize_worker_stems(
                    paths=paths,
                    slug=slug,
                    payload={
                        "ok": True,
                        "status": "succeeded",
                        "stems_dir": str(stem_dir),
                        "stems_uris": stems_uris,
                    },
                    timeout_secs=10.0,
                )

            self.assertEqual(got, stem_dir)
            for name in ("vocals", "bass", "drums", "other"):
                self.assertTrue((stem_dir / f"{name}.wav").exists())

    def test_ensure_demucs_stems_singleflight_dedupes_parallel_worker_calls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            src_mp3 = paths.mp3s / "shared.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=0)

            errors: list[Exception] = []
            results: list[tuple[Path, str]] = []

            def fake_worker(_cfg, payload):  # type: ignore[no-untyped-def]
                _ = _cfg
                time.sleep(0.10)
                stem_dir = Path(str(payload["output_dir"]))
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)
                return {"ok": True, "status": "succeeded", "stems_dir": str(stem_dir)}

            def run_slug(slug: str) -> None:
                try:
                    results.append(
                        step2._ensure_demucs_stems_singleflight(
                            paths,
                            slug,
                            src_mp3,
                            IOFlags(force=False),
                            cfg,
                        )
                    )
                except Exception as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            with (
                mock.patch.object(step2, "DEMUCS_SINGLEFLIGHT_ENABLED", True),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", True),
                mock.patch("scripts.step2_split._request_gpu_worker", side_effect=fake_worker) as worker_mock,
            ):
                t1 = threading.Thread(target=run_slug, args=("song-a",))
                t2 = threading.Thread(target=run_slug, args=("song-b",))
                t1.start()
                t2.start()
                t1.join(timeout=3.0)
                t2.join(timeout=3.0)

            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            self.assertEqual(worker_mock.call_count, 1)
            self.assertEqual(sorted(backend for _stem_dir, backend in results), ["global_stem_cache", "gpu_worker"])
            for slug in ("song-a", "song-b"):
                for name in ("vocals", "bass", "drums", "other"):
                    self.assertTrue((paths.separated / "htdemucs" / slug / f"{name}.wav").exists())

    def test_ensure_demucs_stems_singleflight_can_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            src_mp3 = paths.mp3s / "shared.mp3"
            src_mp3.parent.mkdir(parents=True, exist_ok=True)
            src_mp3.write_bytes(b"source-audio")
            cfg = step2.GPUWorkerConfig(url="https://worker.placeholder.invalid/separate", timeout_secs=30.0, retries=0)

            def fake_worker(_cfg, payload):  # type: ignore[no-untyped-def]
                _ = _cfg
                stem_dir = Path(str(payload["output_dir"]))
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)
                return {"ok": True, "status": "succeeded", "stems_dir": str(stem_dir)}

            with (
                mock.patch.object(step2, "DEMUCS_SINGLEFLIGHT_ENABLED", False),
                mock.patch.object(step2, "GLOBAL_STEM_CACHE_ENABLED", False),
                mock.patch("scripts.step2_split._request_gpu_worker", side_effect=fake_worker) as worker_mock,
            ):
                step2._ensure_demucs_stems_singleflight(
                    paths,
                    "song-a",
                    src_mp3,
                    IOFlags(force=False),
                    cfg,
                )
                step2._ensure_demucs_stems_singleflight(
                    paths,
                    "song-b",
                    src_mp3,
                    IOFlags(force=False),
                    cfg,
                )

            self.assertEqual(worker_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
