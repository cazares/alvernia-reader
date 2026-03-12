import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import step4_assemble as step4


class Step4AssembleTests(unittest.TestCase):
    def test_fast_render_profile_preset_exists(self) -> None:
        self.assertIn("fast", step4._RENDER_PROFILE_PRESETS)
        fast = step4._RENDER_PROFILE_PRESETS["fast"]
        self.assertEqual(fast["size"], "854x480")
        self.assertEqual(fast["fps"], 2)
        self.assertEqual(fast["x264_preset"], "ultrafast")
        self.assertEqual(fast["x264_tune"], "zerolatency")

    def test_turbo_render_profile_preset_exists(self) -> None:
        self.assertIn("turbo", step4._RENDER_PROFILE_PRESETS)
        turbo = step4._RENDER_PROFILE_PRESETS["turbo"]
        self.assertEqual(turbo["x264_preset"], "ultrafast")
        self.assertEqual(turbo["x264_tune"], "zerolatency")
        self.assertEqual(turbo["size"], "854x480")
        self.assertEqual(turbo["fps"], 1)

    def test_slugify(self) -> None:
        self.assertEqual(step4.slugify("  My Song!?  "), "my_song")
        self.assertEqual(step4.slugify(""), "song")

    def test_seconds_to_ass_time(self) -> None:
        self.assertEqual(step4.seconds_to_ass_time(-1.0), "0:00:00.00")
        self.assertEqual(step4.seconds_to_ass_time(61.237), "0:01:01.24")

    def test_rgb_to_bgr(self) -> None:
        self.assertEqual(step4.rgb_to_bgr("112233"), "332211")
        self.assertEqual(step4.rgb_to_bgr("#AABBCC"), "CCBBAA")

    def test_is_music_only(self) -> None:
        self.assertTrue(step4.is_music_only("♪♪"))
        self.assertTrue(step4.is_music_only("..."))
        self.assertTrue(step4.is_music_only("[instrumental break]"))
        self.assertFalse(step4.is_music_only("hello world"))

    def test_read_meta_prefers_step1_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            (meta_dir / "song.step1.json").write_text(
                json.dumps({"artist": "Artist 1", "title": "Title 1"}), encoding="utf-8"
            )
            (meta_dir / "song.json").write_text(
                json.dumps({"artist": "Artist 2", "title": "Title 2"}), encoding="utf-8"
            )
            with mock.patch.object(step4, "META_DIR", meta_dir):
                artist, title = step4.read_meta("song")
        self.assertEqual((artist, title), ("Artist 1", "Title 1"))

    def test_read_meta_handles_bad_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            (meta_dir / "song.step1.json").write_text("{broken json", encoding="utf-8")
            with mock.patch.object(step4, "META_DIR", meta_dir):
                artist, title = step4.read_meta("song")
        self.assertEqual(artist, "")
        self.assertEqual(title, "song")

    def test_read_meta_prefers_query_split_for_card_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            (meta_dir / "song.step1.json").write_text(
                json.dumps(
                    {
                        "artist": "MOSHPIT",
                        "title": "Slipknot - Snuff",
                        "query": "Slipknot - Snuff",
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(step4, "META_DIR", meta_dir):
                artist, title = step4.read_meta("song")
        self.assertEqual((artist, title), ("Slipknot", "Snuff"))

    def test_read_meta_uses_query_as_title_when_meta_title_is_slug_like(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            (meta_dir / "las_ma_anitas.step1.json").write_text(
                json.dumps(
                    {
                        "artist": "",
                        "title": "las_ma_anitas",
                        "query": "Las Mañanitas",
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(step4, "META_DIR", meta_dir):
                artist, title = step4.read_meta("las_ma_anitas")
        self.assertEqual((artist, title), ("", "Las Mañanitas"))

    def test_normalize_title_artist_for_card_handles_noisy_channel_and_duplicate_prefix(self) -> None:
        artist, title = step4._normalize_title_artist_for_card(
            "TheBeatlesVEVO",
            "The Beatles - The Beatles - Let It Be",
        )
        self.assertEqual(artist, "The Beatles")
        self.assertEqual(title, "Let It Be")

    def test_normalize_title_artist_for_card_prefers_title_split_for_handle_like_artist(self) -> None:
        artist, title = step4._normalize_title_artist_for_card(
            "twinkle8539",
            "John Frusciante - God with Lyrics",
        )
        self.assertEqual(artist, "John Frusciante")
        self.assertEqual(title, "God")

    def test_is_metadata_like_preview_line_rejects_title_or_channel_noise(self) -> None:
        self.assertTrue(
            step4._is_metadata_like_preview_line(
                "The Beatles - Let It Be",
                artist="The Beatles",
                title="Let It Be",
            )
        )
        self.assertTrue(
            step4._is_metadata_like_preview_line(
                "John Frusciante - God with Lyrics",
                artist="twinkle8539",
                title="God",
            )
        )
        self.assertFalse(
            step4._is_metadata_like_preview_line(
                "When I find myself in times of trouble",
                artist="The Beatles",
                title="Let It Be",
            )
        )

    def test_read_timings_parses_header_format_and_sorts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            csv_path = timings_dir / "song.csv"
            csv_path.write_text(
                "line_index,time_secs,text\n2,4.0,last\n0,1.0,first\n1,2.5,mid\n",
                encoding="utf-8",
            )
            with mock.patch.object(step4, "TIMINGS_DIR", timings_dir):
                rows = step4.read_timings("song")
        self.assertEqual(rows, [(1.0, "first", 0), (2.5, "mid", 1), (4.0, "last", 2)])

    def test_read_timings_parses_legacy_two_column_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            csv_path = timings_dir / "song.csv"
            csv_path.write_text("time,text\n1.5,hello\n2.0,world\n", encoding="utf-8")
            with mock.patch.object(step4, "TIMINGS_DIR", timings_dir):
                rows = step4.read_timings("song")
        self.assertEqual(rows, [(1.5, "hello", 0), (2.0, "world", 0)])

    def test_resolve_ffprobe_bin_prefers_env(self) -> None:
        with mock.patch.dict("os.environ", {"KARAOKE_FFPROBE_BIN": "/tmp/fp"}, clear=False):
            self.assertEqual(step4.resolve_ffprobe_bin("/usr/bin/ffmpeg"), "/tmp/fp")

    def test_resolve_ffprobe_bin_uses_sibling_binary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ffmpeg = Path(td) / "ffmpeg"
            ffprobe = Path(td) / "ffprobe"
            ffmpeg.write_text("", encoding="utf-8")
            ffprobe.write_text("", encoding="utf-8")
            with mock.patch.dict("os.environ", {}, clear=False):
                self.assertEqual(step4.resolve_ffprobe_bin(str(ffmpeg)), str(ffprobe))

    def test_scaled_bitrate(self) -> None:
        self.assertEqual(step4._scaled_bitrate("100k", 0.5), "50k")
        self.assertEqual(step4._scaled_bitrate("1.2M", 0.5), "600k")
        self.assertEqual(step4._scaled_bitrate("80", 2.0), "160")
        self.assertEqual(step4._scaled_bitrate("oops", 2.0), "oops")

    def test_resolve_video_encoder_prefers_env_override(self) -> None:
        with (
            mock.patch.object(step4, "VIDEO_ENCODER_OVERRIDE", "h264_qsv"),
            mock.patch.object(step4, "FORCE_LIBX264", False),
            mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", return_value=False),
        ):
            self.assertEqual(step4._resolve_video_encoder(Path("ffmpeg")), "h264_qsv")

    def test_resolve_video_encoder_uses_nvenc_on_linux_when_available(self) -> None:
        with (
            mock.patch.object(step4, "VIDEO_ENCODER_OVERRIDE", ""),
            mock.patch.object(step4, "FORCE_LIBX264", False),
            mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
            mock.patch("scripts.step4_assemble.sys.platform", "linux"),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", side_effect=lambda _ffmpeg, name: name == "h264_nvenc"),
            mock.patch("scripts.step4_assemble.shutil.which", return_value="/usr/bin/nvidia-smi"),
            mock.patch("scripts.step4_assemble.run_cmd_capture", return_value=(0, "GPU 0: Test GPU")),
        ):
            self.assertEqual(step4._resolve_video_encoder(Path("ffmpeg")), "h264_nvenc")

    def test_resolve_video_encoder_falls_back_to_libx264_when_unavailable(self) -> None:
        with (
            mock.patch.object(step4, "VIDEO_ENCODER_OVERRIDE", ""),
            mock.patch.object(step4, "FORCE_LIBX264", False),
            mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
            mock.patch("scripts.step4_assemble.sys.platform", "darwin"),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", return_value=False),
        ):
            self.assertEqual(step4._resolve_video_encoder(Path("ffmpeg")), "libx264")

    def test_resolve_audio_encoder_prefers_env_override(self) -> None:
        with (
            mock.patch.object(step4, "AUDIO_ENCODER_OVERRIDE", "libfdk_aac"),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", return_value=False),
        ):
            self.assertEqual(step4._resolve_audio_encoder(Path("ffmpeg")), "libfdk_aac")

    def test_resolve_audio_encoder_prefers_aac_at_on_darwin_when_available(self) -> None:
        with (
            mock.patch.object(step4, "AUDIO_ENCODER_OVERRIDE", ""),
            mock.patch("scripts.step4_assemble.sys.platform", "darwin"),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", side_effect=lambda _ffmpeg, name: name == "aac_at"),
        ):
            self.assertEqual(step4._resolve_audio_encoder(Path("ffmpeg")), "aac_at")

    def test_resolve_audio_encoder_falls_back_to_aac(self) -> None:
        with (
            mock.patch.object(step4, "AUDIO_ENCODER_OVERRIDE", ""),
            mock.patch("scripts.step4_assemble.sys.platform", "linux"),
            mock.patch("scripts.step4_assemble.ffmpeg_has_encoder", return_value=False),
        ):
            self.assertEqual(step4._resolve_audio_encoder(Path("ffmpeg")), "aac")

    def test_remove_flag_and_value_removes_all_occurrences(self) -> None:
        cmd = ["ffmpeg", "-preset", "slow", "-x", "1", "-preset", "fast"]
        step4._remove_flag_and_value(cmd, "-preset")
        self.assertEqual(cmd, ["ffmpeg", "-x", "1"])

    def test_probe_media_duration_secs(self) -> None:
        with mock.patch("scripts.step4_assemble.run_cmd_capture", return_value=(0, "12.34")):
            dur, err = step4._probe_media_duration_secs(Path("/tmp/in.mp4"), "ffprobe")
        self.assertEqual(err, "")
        self.assertAlmostEqual(dur or 0.0, 12.34)

        with mock.patch("scripts.step4_assemble.run_cmd_capture", return_value=(1, "bad")):
            dur, err = step4._probe_media_duration_secs(Path("/tmp/in.mp4"), "ffprobe")
        self.assertIsNone(dur)
        self.assertIn("failed", err)

    def test_validate_render_output(self) -> None:
        ok, msg = step4._validate_render_output(
            Path("/tmp/nope.mp4"),
            "ffprobe",
            expected_audio_duration=0.0,
            require_ffprobe=True,
        )
        self.assertFalse(ok)
        self.assertIn("missing", msg)

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp4"
            out.write_bytes(b"x")
            ok2, msg2 = step4._validate_render_output(
                out,
                "ffprobe",
                expected_audio_duration=0.0,
                require_ffprobe=True,
            )
            self.assertFalse(ok2)
            self.assertIn("too small", msg2)

            out.write_bytes(b"x" * 5000)
            with (
                mock.patch.dict(
                    "os.environ",
                    {"KARAOKE_MIN_MP4_BYTES": "1024", "KARAOKE_MIN_MP4_DURATION_SEC": "0.5"},
                    clear=False,
                ),
                mock.patch("scripts.step4_assemble._probe_media_duration_secs", return_value=(3.0, "")),
            ):
                ok3, msg3 = step4._validate_render_output(
                    out,
                    "ffprobe",
                    expected_audio_duration=2.0,
                    require_ffprobe=True,
                )
            self.assertTrue(ok3)
            self.assertEqual(msg3, "")

            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_MIN_MP4_BYTES": "1024",
                        "KARAOKE_MIN_MP4_DURATION_SEC": "0.5",
                        "KARAOKE_MIN_DURATION_RATIO": "0.75",
                    },
                    clear=False,
                ),
                mock.patch("scripts.step4_assemble._probe_media_duration_secs", return_value=(5.0, "")),
            ):
                ok4, msg4 = step4._validate_render_output(
                    out,
                    "ffprobe",
                    expected_audio_duration=20.0,
                    require_ffprobe=True,
                )
            self.assertFalse(ok4)
            self.assertIn("vs audio", msg4)

    def test_file_fingerprint_and_stable_hash(self) -> None:
        missing = step4._file_fingerprint(Path("/tmp/definitely_missing_file.xyz"))
        self.assertFalse(missing["exists"])

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "a.txt"
            p.write_text("hello", encoding="utf-8")
            fp = step4._file_fingerprint(p)
            self.assertTrue(fp["exists"])
            self.assertEqual(fp["size"], 5)

        h1 = step4._stable_hash_obj({"b": 2, "a": 1})
        h2 = step4._stable_hash_obj({"a": 1, "b": 2})
        self.assertEqual(h1, h2)

    def test_try_load_json_and_write_json_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "obj.json"
            self.assertIsNone(step4._try_load_json(p))
            step4._write_json_atomic(p, {"x": 1})
            self.assertEqual(step4._try_load_json(p), {"x": 1})
            p.write_text("{bad", encoding="utf-8")
            self.assertIsNone(step4._try_load_json(p))

    def test_probe_audio_duration_uses_cache_when_fingerprint_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            audio = Path(td) / "song.wav"
            audio.write_bytes(b"wav")

            with mock.patch.object(step4, "OUTPUT_DIR", output_dir):
                fp = step4._file_fingerprint(audio)
                cache = output_dir / "song.duration.cache.json"
                cache.write_text(json.dumps({"fingerprint": fp, "duration_secs": 12.5}), encoding="utf-8")
                with mock.patch("scripts.step4_assemble.run_cmd_capture", side_effect=AssertionError("should not probe")):
                    dur = step4.probe_audio_duration(audio, slug="song")
        self.assertEqual(dur, 12.5)

    def test_probe_audio_duration_writes_cache_on_probe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            audio = Path(td) / "song.wav"
            audio.write_bytes(b"wav")

            with (
                mock.patch.object(step4, "OUTPUT_DIR", output_dir),
                mock.patch("scripts.step4_assemble.resolve_ffmpeg_bin", return_value="ffmpeg"),
                mock.patch("scripts.step4_assemble.resolve_ffprobe_bin", return_value="ffprobe"),
                mock.patch("scripts.step4_assemble.run_cmd_capture", return_value=(0, "7.5")),
            ):
                dur = step4.probe_audio_duration(audio, slug="song")

            self.assertAlmostEqual(dur, 7.5)
            cache = json.loads((output_dir / "song.duration.cache.json").read_text(encoding="utf-8"))
            self.assertEqual(cache["duration_secs"], 7.5)

    def test_compute_default_title_card_lines(self) -> None:
        self.assertEqual(step4.compute_default_title_card_lines("slug_name", "A", "T"), ["T", "", "by", "", "A"])
        self.assertEqual(step4.compute_default_title_card_lines("slug_name", "A", "T", connector_word="de"), ["T", "", "de", "", "A"])
        self.assertEqual(step4.compute_default_title_card_lines("slug_name", "", ""), ["Slug Name"])

    def test_parse_title_card_display_lines_honors_literal_newline_sequence(self) -> None:
        self.assertEqual(step4._parse_title_card_display_lines("Line 1\\n\\nLine 2"), ["Line 1", "", "Line 2"])

    def test_title_card_credit_text_is_language_aware(self) -> None:
        self.assertIn("This video was architected, engineered, and auto-generated", step4._title_card_credit_text_for_connector("by"))
        self.assertIn("Este video fue diseñado, ideado, y generado automáticamente", step4._title_card_credit_text_for_connector("de"))

    def test_build_ass_skips_next_overlay_during_intro_window(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            timings = [
                (0.0, "", 0),
                (10.0, "Line one", 1),
                (20.0, "Line two", 2),
            ]
            with (
                mock.patch.object(step4, "OUTPUT_DIR", out_dir),
                mock.patch.object(step4, "LYRICS_OFFSET_SECS", 0.0),
            ):
                ass_path = step4.build_ass(
                    slug="song",
                    artist="The Beatles",
                    title="Let It Be",
                    timings=timings,
                    audio_duration=30.0,
                    font_name="Helvetica",
                    font_size_script=80,
                    title_card_lines=["Let It Be", "", "by", "", "The Beatles"],
                )
            text = ass_path.read_text(encoding="utf-8")
            next_lines = [line for line in text.splitlines() if "Next:" in line]
            self.assertTrue(next_lines)
            for line in next_lines:
                self.assertNotIn(",0:00:00.00,", line)
                self.assertNotIn(",0:00:05.00,", line)
            self.assertIn("This video was architected, engineered, and auto-generated by Miguel Cázares", text)
            self.assertIn("{\\an2\\pos(", text)

    def test_build_ass_uses_spanish_credits_when_timings_are_spanish(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            timings = [
                (3.0, "que linda esta la mañana", 0),
                (6.0, "de hoy", 1),
            ]
            with (
                mock.patch.object(step4, "OUTPUT_DIR", out_dir),
                mock.patch.object(step4, "LYRICS_OFFSET_SECS", 0.0),
            ):
                ass_path = step4.build_ass(
                    slug="song_es",
                    artist="Alfonso Esparza Oteo",
                    title="Las Mañanitas",
                    timings=timings,
                    audio_duration=12.0,
                    font_name="Helvetica",
                    font_size_script=80,
                )
            text = ass_path.read_text(encoding="utf-8")
            self.assertIn("Este video fue diseñado, ideado, y generado automáticamente por Miguel Cázares", text)

    def test_escape_drawtext_text(self) -> None:
        self.assertEqual(step4._escape_drawtext_text("a:b'c%\\d"), "a\\:b\\'c\\%\\\\d")

    def test_clamp_offset_secs(self) -> None:
        self.assertEqual(step4.clamp_offset_secs(4.5), 4.5)
        self.assertEqual(step4.clamp_offset_secs(step4.OFFSET_SECS_MAX + 1.0), step4.OFFSET_SECS_MAX)
        self.assertEqual(step4.clamp_offset_secs(step4.OFFSET_SECS_MIN - 1.0), step4.OFFSET_SECS_MIN)
        self.assertEqual(step4.clamp_offset_secs("bad"), 0.0)

    def test_build_drawtext_vf(self) -> None:
        vf = step4.build_drawtext_vf(
            [(1.0, "First line", 0), (2.0, "♪", 1)],
            audio_duration=3.0,
            font="Helvetica",
            fontsize=42,
            offset_secs=0.0,
        )
        self.assertIn("between(t,1.000,2.000)", vf)
        self.assertIn("between(t,2.000,3.000)", vf)
        self.assertIn("y=h*0.70", vf)

    def test_build_drawtext_vf_clips_positive_offset_at_track_end(self) -> None:
        vf = step4.build_drawtext_vf(
            [(1.0, "Line one", 0), (11.0, "Line two", 1)],
            audio_duration=12.0,
            font="Helvetica",
            fontsize=42,
            offset_secs=10.0,
        )
        self.assertIn("between(t,11.000,12.000)", vf)
        self.assertNotIn("between(t,11.000,21.000)", vf)
        self.assertNotIn("between(t,21.000", vf)

    def test_build_drawtext_vf_clips_negative_offset_before_track_start(self) -> None:
        vf = step4.build_drawtext_vf(
            [(1.0, "Line one", 0), (2.0, "Line two", 1)],
            audio_duration=4.0,
            font="Helvetica",
            fontsize=42,
            offset_secs=-10.0,
        )
        self.assertNotIn("between(t,-", vf)
        self.assertIn("between(t,0.000,4.000)", vf)

    def test_build_ass_clips_dialogue_windows_to_audio_duration(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            timings = [
                (1.0, "Line one", 0),
                (11.0, "Line two", 1),
            ]
            with (
                mock.patch.object(step4, "OUTPUT_DIR", out_dir),
                mock.patch.object(step4, "LYRICS_OFFSET_SECS", 10.0),
            ):
                ass_path = step4.build_ass(
                    slug="song",
                    artist="Artist",
                    title="Title",
                    timings=timings,
                    audio_duration=12.0,
                    font_name="Helvetica",
                    font_size_script=80,
                )
            text = ass_path.read_text(encoding="utf-8")

            def parse_ass_time(value: str) -> float:
                h, m, rest = value.split(":")
                s, cs = rest.split(".")
                return (int(h) * 3600.0) + (int(m) * 60.0) + int(s) + (int(cs) / 100.0)

            for line in text.splitlines():
                if not line.startswith("Dialogue:"):
                    continue
                m = re.match(r"^Dialogue:\s*\d+,([^,]+),([^,]+),", line)
                self.assertIsNotNone(m)
                if not m:
                    continue
                start = parse_ass_time(m.group(1))
                end = parse_ass_time(m.group(2))
                self.assertGreaterEqual(start, 0.0)
                self.assertGreaterEqual(end, start)
                self.assertLessEqual(end, 12.01)

    def test_estimate_mute_duration_from_timings_uses_last_timestamp_plus_padding(self) -> None:
        duration = step4._estimate_mute_duration_from_timings(
            [(1.0, "first", 0), (9.5, "last", 1)],
            min_secs=8.0,
            pad_secs=5.0,
        )
        self.assertAlmostEqual(duration, 14.5)

    def test_estimate_mute_duration_from_timings_uses_minimum_when_empty(self) -> None:
        duration = step4._estimate_mute_duration_from_timings([], min_secs=7.0, pad_secs=5.0)
        self.assertAlmostEqual(duration, 7.0)

    def test_parse_args_accepts_mute_flag(self) -> None:
        args = step4.parse_args(["--slug", "song", "--mute"])
        self.assertTrue(args.mute)

    def test_parse_args_accepts_title_card_display(self) -> None:
        args = step4.parse_args(["--slug", "song", "--title-card-display", "Line 1\\nLine 2"])
        self.assertEqual(args.title_card_display, "Line 1\\nLine 2")

    def test_parse_args_accepts_font_size_percent(self) -> None:
        args = step4.parse_args(["--slug", "song", "--font-size-percent", "75"])
        self.assertAlmostEqual(float(args.font_size_percent), 75.0, places=3)

    def test_parse_args_accepts_prefer_step1_audio_flag(self) -> None:
        args = step4.parse_args(["--slug", "song", "--prefer-step1-audio"])
        self.assertTrue(args.prefer_step1_audio)

    def test_peek_first_time_secs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "timings.csv"
            p.write_text("line_index,time_secs,text\n0,1.250,hi\n", encoding="utf-8")
            self.assertEqual(step4._peek_first_time_secs(p), 1.25)

    def test_choose_audio_prefers_wav_then_mp3(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mixes_dir = Path(td)
            wav = mixes_dir / "song.wav"
            mp3 = mixes_dir / "song.mp3"
            wav.write_bytes(b"wav")
            mp3.write_bytes(b"mp3")
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=None),
                mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
                mock.patch.dict("os.environ", {"KARAOKE_PREFER_MP3_FOR_RENDER": "0"}, clear=False),
            ):
                self.assertEqual(step4.choose_audio("song"), wav)
            wav.unlink()
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=None),
                mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
                mock.patch.dict("os.environ", {"KARAOKE_PREFER_MP3_FOR_RENDER": "0"}, clear=False),
            ):
                self.assertEqual(step4.choose_audio("song"), mp3)
            mp3.unlink()
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=None),
                mock.patch.object(step4, "RENDER_PROFILE_NAME", ""),
                mock.patch.dict("os.environ", {"KARAOKE_PREFER_MP3_FOR_RENDER": "0"}, clear=False),
            ):
                with self.assertRaises(SystemExit):
                    step4.choose_audio("song")

    def test_choose_audio_prefers_mp3_in_turbo_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mixes_dir = Path(td)
            wav = mixes_dir / "song.wav"
            mp3 = mixes_dir / "song.mp3"
            wav.write_bytes(b"wav")
            mp3.write_bytes(b"mp3")
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=None),
                mock.patch.object(step4, "RENDER_PROFILE_NAME", "turbo"),
                mock.patch.dict("os.environ", {"KARAOKE_PREFER_MP3_FOR_RENDER": ""}, clear=False),
            ):
                self.assertEqual(step4.choose_audio("song"), mp3)

    def test_choose_audio_prefers_prepared_render_audio_in_fast_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mixes_dir = Path(td)
            wav = mixes_dir / "song.wav"
            wav.write_bytes(b"wav")
            prepared = mixes_dir / "song.m4a"
            prepared.write_bytes(b"m4a")
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=None),
                mock.patch.object(step4, "RENDER_PROFILE_NAME", "fast"),
                mock.patch(
                    "scripts.step2_split.wait_for_prepared_render_audio",
                    return_value=prepared,
                ) as wait_mock,
            ):
                self.assertEqual(step4.choose_audio("song"), prepared)
            wait_mock.assert_called_once()

    def test_choose_audio_falls_back_to_step1_audio(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mixes_dir = Path(td) / "mixes"
            mixes_dir.mkdir(parents=True, exist_ok=True)
            step1_audio = Path(td) / "song.m4a"
            step1_audio.write_bytes(b"audio")
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=step1_audio),
            ):
                self.assertEqual(step4.choose_audio("song"), step1_audio)

    def test_choose_audio_prefers_step1_audio_when_flag_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mixes_dir = Path(td)
            wav = mixes_dir / "song.wav"
            wav.write_bytes(b"wav")
            step1_audio = Path(td) / "song.m4a"
            step1_audio.write_bytes(b"audio")
            with (
                mock.patch.object(step4, "MIXES_DIR", mixes_dir),
                mock.patch("scripts.step4_assemble._resolve_step1_audio", return_value=step1_audio),
            ):
                self.assertEqual(step4.choose_audio("song", prefer_step1_audio=True), step1_audio)

    def test_audio_stream_copy_supported_for_mp3_symlink_to_mp4(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            target = base / "song.mp4"
            alias = base / "song.mp3"
            target.write_bytes(b"mp4")
            alias.symlink_to(target)
            self.assertTrue(step4._audio_stream_copy_supported(alias))

    def test_audio_stream_copy_supported_for_aac_lc_source_when_ffprobe_confirms_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            source = base / "song.m4a"
            source.write_bytes(b"m4a")
            with mock.patch(
                "scripts.step4_assemble.run_cmd_capture",
                return_value=(0, json.dumps({"streams": [{"codec_type": "audio", "codec_name": "aac", "profile": "LC"}]})),
            ):
                self.assertTrue(step4._audio_stream_copy_supported(source, ffprobe_bin="ffprobe"))

    def test_audio_stream_copy_not_supported_for_he_aac_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            source = base / "song.m4a"
            source.write_bytes(b"m4a")
            with mock.patch(
                "scripts.step4_assemble.run_cmd_capture",
                return_value=(0, json.dumps({"streams": [{"codec_type": "audio", "codec_name": "aac", "profile": "HE-AAC"}]})),
            ):
                self.assertFalse(step4._audio_stream_copy_supported(source, ffprobe_bin="ffprobe"))

    def test_audio_stream_copy_not_supported_for_raw_mp3_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "song.mp3"
            p.write_bytes(b"mp3")
            self.assertFalse(step4._audio_stream_copy_supported(p))

    def test_audio_encoder_profile_args_omits_profile_for_aac_at(self) -> None:
        self.assertEqual(step4._audio_encoder_profile_args("aac_at"), [])

    def test_audio_encoder_profile_args_pins_aac_low_for_native_aac(self) -> None:
        self.assertEqual(step4._audio_encoder_profile_args("aac"), ["-profile:a", "aac_low"])

    def test_validate_render_output_can_skip_duration_probe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp4"
            out.write_bytes(b"x" * 5000)
            with (
                mock.patch.object(step4, "RENDER_PROFILE_NAME", "turbo"),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_MIN_MP4_BYTES": "1024",
                        "KARAOKE_TURBO_VALIDATE_DURATION": "0",
                        "KARAOKE_VALIDATE_DURATION": "",
                    },
                    clear=False,
                ),
                mock.patch("scripts.step4_assemble._probe_media_duration_secs", side_effect=AssertionError("should not probe")),
            ):
                ok, msg = step4._validate_render_output(
                    out,
                    "ffprobe",
                    expected_audio_duration=20.0,
                    require_ffprobe=True,
                )
        self.assertTrue(ok)
        self.assertEqual(msg, "")

    def test_ass_inputs_digest_changes_when_inputs_change(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_csv = base / "song.csv"
            meta1 = base / "song.step1.json"
            meta2 = base / "song.json"
            timings_csv.write_text("line_index,time_secs,text\n", encoding="utf-8")
            meta1.write_text("{}", encoding="utf-8")
            meta2.write_text("{}", encoding="utf-8")
            audio_fp = {"path": "/tmp/a.wav", "exists": True, "size": 1, "mtime": 1.0}

            d1 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=40,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="",
            )
            d2 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=41,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="",
            )
        self.assertNotEqual(d1, d2)

    def test_ass_inputs_digest_changes_when_title_card_display_changes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_csv = base / "song.csv"
            meta1 = base / "song.step1.json"
            meta2 = base / "song.json"
            timings_csv.write_text("line_index,time_secs,text\n", encoding="utf-8")
            meta1.write_text("{}", encoding="utf-8")
            meta2.write_text("{}", encoding="utf-8")
            audio_fp = {"path": "/tmp/a.wav", "exists": True, "size": 1, "mtime": 1.0}

            d1 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=40,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="Line 1",
            )
            d2 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=40,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="Line 1\\nLine 2",
            )
        self.assertNotEqual(d1, d2)

    def test_ass_inputs_digest_changes_when_title_card_font_percent_changes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_csv = base / "song.csv"
            meta1 = base / "song.step1.json"
            meta2 = base / "song.json"
            timings_csv.write_text("line_index,time_secs,text\n", encoding="utf-8")
            meta1.write_text("{}", encoding="utf-8")
            meta2.write_text("{}", encoding="utf-8")
            audio_fp = {"path": "/tmp/a.wav", "exists": True, "size": 1, "mtime": 1.0}

            d1 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=40,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="Line 1",
                title_card_font_percent=100.0,
            )
            d2 = step4._ass_inputs_digest(
                "song",
                timings_csv=timings_csv,
                meta1=meta1,
                meta2=meta2,
                audio_fp=audio_fp,
                ass_font_size=40,
                font_name="Helvetica",
                offset_secs=0.0,
                title_card_display="Line 1",
                title_card_font_percent=75.0,
            )
        self.assertNotEqual(d1, d2)

    def test_build_ass_applies_title_card_font_percent_to_intro_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            with (
                mock.patch.object(step4, "OUTPUT_DIR", out_dir),
                mock.patch.object(step4, "LYRICS_OFFSET_SECS", 0.0),
            ):
                ass_path = step4.build_ass(
                    "song_fs",
                    "Artist",
                    "Title",
                    timings=[(5.0, "First", 0)],
                    audio_duration=10.0,
                    font_name="Helvetica",
                    font_size_script=80,
                    title_card_font_percent=75.0,
                )
            text = ass_path.read_text(encoding="utf-8")
            self.assertIn("{\\an5\\pos(427,240)\\fs60}", text)

    def test_build_ass_generates_output_for_empty_and_non_empty_timings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            with mock.patch.object(step4, "OUTPUT_DIR", out_dir):
                ass_empty = step4.build_ass(
                    "song",
                    "Artist",
                    "Title",
                    timings=[],
                    audio_duration=0.0,
                    font_name="Helvetica",
                    font_size_script=40,
                )
                txt_empty = ass_empty.read_text(encoding="utf-8")
                self.assertIn("by", txt_empty)

                ass_non_empty = step4.build_ass(
                    "song2",
                    "Artist",
                    "Title",
                    timings=[(1.0, "First", 0), (2.0, "Second", 1)],
                    audio_duration=3.0,
                    font_name="Helvetica",
                    font_size_script=40,
                )
                txt_non_empty = ass_non_empty.read_text(encoding="utf-8")
                self.assertIn("First", txt_non_empty)
                self.assertIn("Next:", txt_non_empty)


if __name__ == "__main__":
    unittest.main()
