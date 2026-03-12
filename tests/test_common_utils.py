import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import common


class CommonUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        common.clear_ffmpeg_capability_cache()

    def test_ensure_dir_creates_nested_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "a" / "b" / "c"
            self.assertFalse(target.exists())
            common.ensure_dir(target)
            self.assertTrue(target.exists())
            self.assertTrue(target.is_dir())

    def test_slugify_normalizes_text(self) -> None:
        self.assertEqual(common.slugify("  Hello, World!  "), "hello_world")
        self.assertEqual(common.slugify("a___b"), "a_b")
        self.assertEqual(common.slugify(""), "song")

    def test_resolve_output_dir_defaults_to_output(self) -> None:
        with tempfile.TemporaryDirectory() as td, mock.patch.dict("os.environ", {"MIXTERIOSO_OUTPUT_SUBDIR": ""}, clear=False):
            root = Path(td)
            self.assertEqual(common.resolve_output_dir(root), root / "output")

    def test_resolve_output_dir_supports_relative_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            got = common.resolve_output_dir(root, output_subdir="output/temp")
            self.assertEqual(got, root / "output" / "temp")

    def test_paths_output_subdir_is_respected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = common.Paths(root=root, output_subdir="output/temp")
            self.assertEqual(paths.output, root / "output" / "temp")
            paths.ensure()
            self.assertTrue(paths.output.exists())

    def test_should_write_true_when_force_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "file.txt"
            p.write_text("x", encoding="utf-8")
            flags = common.IOFlags(force=True)
            self.assertTrue(common.should_write(p, flags, label="file"))

    def test_should_write_true_when_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "missing.txt"
            flags = common.IOFlags()
            self.assertTrue(common.should_write(p, flags, label="file"))

    def test_should_write_uses_confirm_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "exists.txt"
            p.write_text("x", encoding="utf-8")
            flags = common.IOFlags(confirm=True)
            with mock.patch("builtins.input", return_value="yes"):
                self.assertTrue(common.should_write(p, flags, label="file"))
            with mock.patch("builtins.input", return_value="n"):
                self.assertFalse(common.should_write(p, flags, label="file"))

    def test_should_write_logs_skip_for_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "exists.txt"
            p.write_text("x", encoding="utf-8")
            flags = common.IOFlags(confirm=False, force=False)
            with mock.patch("scripts.common.log") as log_mock:
                self.assertFalse(common.should_write(p, flags, label="file"))
            log_mock.assert_called_once()

    def test_write_text_respects_should_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x" / "y.txt"
            with mock.patch("scripts.common.should_write", return_value=False):
                common.write_text(p, "hello", common.IOFlags())
            self.assertFalse(p.exists())

    def test_write_text_creates_parent_and_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x" / "y.txt"
            common.write_text(p, "hello", common.IOFlags())
            self.assertEqual(p.read_text(encoding="utf-8"), "hello")

    def test_write_json_creates_parent_and_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x" / "y.json"
            common.write_json(p, {"a": 1}, common.IOFlags())
            self.assertEqual(json.loads(p.read_text(encoding="utf-8")), {"a": 1})

    def test_run_cmd_dry_run_returns_successful_completed_process(self) -> None:
        cp = common.run_cmd(["echo", "hi"], dry_run=True, tag="TEST")
        self.assertIsInstance(cp, subprocess.CompletedProcess)
        self.assertEqual(cp.returncode, 0)

    def test_run_cmd_capture_dry_run_returns_zero_and_empty_output(self) -> None:
        rc, out = common.run_cmd_capture(["echo", "hi"], dry_run=True, tag="TEST")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "")

    def test_resolve_ffmpeg_bin_uses_environment_precedence(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"FFMPEG_BIN": "/tmp/from_env_1", "MIXTERIOSO_FFMPEG": "/tmp/from_env_2", "KARAOKE_FFMPEG": "/tmp/from_env_3"},
            clear=False,
        ):
            got = common.resolve_ffmpeg_bin()
        self.assertEqual(str(got), "/tmp/from_env_1")

    def test_resolve_ffmpeg_bin_falls_back_to_shutil_which(self) -> None:
        with mock.patch.dict("os.environ", {"FFMPEG_BIN": "", "MIXTERIOSO_FFMPEG": "", "KARAOKE_FFMPEG": ""}, clear=False), mock.patch(
            "scripts.common.shutil.which", return_value="/usr/local/bin/ffmpeg"
        ):
            got = common.resolve_ffmpeg_bin()
        self.assertEqual(str(got), "/usr/local/bin/ffmpeg")

    def test_ffmpeg_has_filter_handles_error_and_success_paths(self) -> None:
        with mock.patch("scripts.common.run_cmd_capture", return_value=(1, "oops")):
            self.assertFalse(common.ffmpeg_has_filter(Path("ffmpeg"), "drawtext"))

        out = " ... drawtext ...\n ... subtitles ...\n"
        with mock.patch("scripts.common.run_cmd_capture", return_value=(0, out)):
            self.assertTrue(common.ffmpeg_has_filter(Path("ffmpeg"), "drawtext"))
            self.assertTrue(common.ffmpeg_has_filter(Path("ffmpeg"), "subtitles"))
            self.assertFalse(common.ffmpeg_has_filter(Path("ffmpeg"), "nonexistent"))

    def test_ffmpeg_has_filter_reuses_cached_output(self) -> None:
        out = " ... drawtext ...\n ... subtitles ...\n"
        with mock.patch("scripts.common.run_cmd_capture", return_value=(0, out)) as run_mock:
            self.assertTrue(common.ffmpeg_has_filter(Path("ffmpeg"), "drawtext"))
            self.assertTrue(common.ffmpeg_has_filter(Path("ffmpeg"), "subtitles"))
        self.assertEqual(run_mock.call_count, 1)

    def test_ffmpeg_has_encoder_handles_error_and_success_paths(self) -> None:
        with mock.patch("scripts.common.run_cmd_capture", return_value=(1, "oops")):
            self.assertFalse(common.ffmpeg_has_encoder(Path("ffmpeg"), "h264_nvenc"))

        out = " V....D h264_nvenc NVIDIA NVENC H.264 encoder\n V....D libx264 H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10\n"
        with mock.patch("scripts.common.run_cmd_capture", return_value=(0, out)):
            self.assertTrue(common.ffmpeg_has_encoder(Path("ffmpeg"), "h264_nvenc"))
            self.assertTrue(common.ffmpeg_has_encoder(Path("ffmpeg"), "libx264"))
            self.assertFalse(common.ffmpeg_has_encoder(Path("ffmpeg"), "h264_videotoolbox"))

    def test_ffmpeg_has_encoder_reuses_cached_output(self) -> None:
        out = " V....D h264_nvenc NVIDIA NVENC H.264 encoder\n V....D libx264 H.264 encoder\n"
        with mock.patch("scripts.common.run_cmd_capture", return_value=(0, out)) as run_mock:
            self.assertTrue(common.ffmpeg_has_encoder(Path("ffmpeg"), "h264_nvenc"))
            self.assertTrue(common.ffmpeg_has_encoder(Path("ffmpeg"), "libx264"))
        self.assertEqual(run_mock.call_count, 1)

    def test_ffmpeg_escape_filter_path_escapes_special_characters(self) -> None:
        p = Path("/tmp/a:b'c\\d.ass")
        escaped = common.ffmpeg_escape_filter_path(p)
        self.assertEqual(escaped, "/tmp/a\\:b\\'c\\\\d.ass")

    def test_resolve_demucs_bin_prefers_current_python_environment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_root = Path(td) / "demucs_env"
            bin_dir = env_root / "bin"
            python_bin = bin_dir / "python"
            demucs_bin = bin_dir / "demucs"
            bin_dir.mkdir(parents=True, exist_ok=True)
            python_bin.write_text("", encoding="utf-8")
            demucs_bin.write_text("", encoding="utf-8")

            with (
                mock.patch.dict("os.environ", {"DEMUCS_BIN": "", "MIXTERIOSO_DEMUCS": "", "VIRTUAL_ENV": ""}, clear=False),
                mock.patch.object(common, "ROOT", Path(td) / "repo_root"),
                mock.patch.object(common.sys, "executable", str(python_bin)),
                mock.patch("scripts.common.shutil.which", return_value=None),
            ):
                got = common.resolve_demucs_bin()

        self.assertEqual(got, demucs_bin)

    def test_resolve_demucs_bin_uses_virtual_env_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_root = Path(td) / "active_env"
            demucs_bin = env_root / "bin" / "demucs"
            demucs_bin.parent.mkdir(parents=True, exist_ok=True)
            demucs_bin.write_text("", encoding="utf-8")

            with (
                mock.patch.dict("os.environ", {"DEMUCS_BIN": "", "MIXTERIOSO_DEMUCS": "", "VIRTUAL_ENV": str(env_root)}, clear=False),
                mock.patch.object(common, "ROOT", Path(td) / "repo_root"),
                mock.patch.object(common.sys, "executable", "/nonexistent/python"),
                mock.patch("scripts.common.shutil.which", return_value=None),
            ):
                got = common.resolve_demucs_bin()

        self.assertEqual(got, demucs_bin)


if __name__ == "__main__":
    unittest.main()
