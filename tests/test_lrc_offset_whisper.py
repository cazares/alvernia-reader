from __future__ import annotations

import os
import tempfile
import unittest
import wave
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

from scripts import lrc_offset_whisper as whisper_offset


class LrcOffsetWhisperTests(unittest.TestCase):
    def _write_silent_wav(self, path: Path, duration_s: float) -> None:
        frame_rate = 16000
        frame_count = max(1, int(round(float(duration_s) * frame_rate)))
        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(frame_rate)
            wav_file.writeframes(b"\x00\x00" * frame_count)

    def test_transcribe_stitched_clip_segments_maps_segments_back_to_original_clip_times(self) -> None:
        def fake_ffmpeg_clip(_ffmpeg_bin: str, _audio_in: Path, wav_out: Path, start_s: float, dur_s: float) -> None:
            self._write_silent_wav(wav_out, dur_s)

        with (
            mock.patch.object(whisper_offset, "_ffmpeg_clip_to_wav", side_effect=fake_ffmpeg_clip),
            mock.patch.object(
                whisper_offset,
                "_transcribe_wav_segments",
                return_value=[
                    (0.10, 0.40, "first"),
                    (1.60, 1.90, "second"),
                ],
            ),
            mock.patch.dict(os.environ, {"KARAOKE_AUTO_OFFSET_BATCH_STITCH_GAP_SECS": "0.5"}, clear=False),
        ):
            results = whisper_offset._transcribe_stitched_clip_segments(
                audio_path=Path("song.m4a"),
                language="en",
                ffmpeg_bin="ffmpeg",
                whisper_bin="whisper-cli",
                model_path="model.bin",
                clip_ranges=[(10.0, 1.0), (20.0, 1.0)],
            )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], [(10.10, 10.40, "first")])
        self.assertEqual(results[1], [(20.10, 20.40, "second")])

    def test_estimate_offsets_batch_prefers_stitched_transcription_for_multiple_merged_specs(self) -> None:
        anchor_specs = [
            {
                "anchor_time_s": 1.0,
                "clip_start_s": 0.0,
                "clip_end_s": 10.0,
                "window_rows": [{"id": 1}],
                "frequent_tokens": set(),
            },
            {
                "anchor_time_s": 50.0,
                "clip_start_s": 50.0,
                "clip_end_s": 60.0,
                "window_rows": [{"id": 2}],
                "frequent_tokens": set(),
            },
        ]
        with tempfile.TemporaryDirectory() as td:
            lrc_path = Path(td) / "song.lrc"
            lrc_path.write_text("[00:01.00]one\n[00:50.00]two\n", encoding="utf-8")
            with (
                mock.patch("scripts.lrc_offset_whisper.parse_lrc", return_value=(["dummy"], {})),
                mock.patch.object(whisper_offset, "_collect_usable_lrc_candidates", return_value=[(1.0, "one"), (50.0, "two")]),
                mock.patch.object(whisper_offset, "_build_window_rows", side_effect=anchor_specs),
                mock.patch.object(
                    whisper_offset,
                    "_merge_anchor_specs",
                    return_value=[
                        {"clip_start_s": 0.0, "clip_end_s": 10.0, "spec_indices": [0]},
                        {"clip_start_s": 50.0, "clip_end_s": 60.0, "spec_indices": [1]},
                    ],
                ),
                mock.patch.object(
                    whisper_offset,
                    "_transcribe_stitched_clip_segments",
                    return_value=[
                        [(0.10, 0.40, "one")],
                        [(50.10, 50.40, "two")],
                    ],
                ) as stitched_mock,
                mock.patch.object(whisper_offset, "_transcribe_clip_segments") as serial_mock,
                mock.patch.object(whisper_offset, "_build_segment_rows", return_value=[{"anchor_abs": 0.1, "tokens": ["x"]}]),
                mock.patch.object(
                    whisper_offset,
                    "_estimate_offset_from_rows",
                    side_effect=[(0.1, 0.9), (0.2, 0.8)],
                ),
                mock.patch.dict(os.environ, {"KARAOKE_AUTO_OFFSET_BATCH_STITCH": "1"}, clear=False),
            ):
                results = whisper_offset.estimate_offsets_batch(
                    lrc_path=lrc_path,
                    audio_path=Path("song.m4a"),
                    language="en",
                    ffmpeg_bin="ffmpeg",
                    whisper_bin="whisper-cli",
                    model_path="model.bin",
                    clip_dur_s=35.0,
                    max_abs_offset=90.0,
                    anchor_times_s=[1.0, 50.0],
                    return_confidence=True,
                )

        stitched_mock.assert_called_once()
        serial_mock.assert_not_called()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["error"], None)
        self.assertAlmostEqual(float(results[0]["offset_s"]), 0.1, places=3)
        self.assertAlmostEqual(float(results[1]["offset_s"]), 0.2, places=3)

    def test_estimate_offset_from_rows_can_reuse_same_segment_for_multiple_lines(self) -> None:
        window_rows = [
            {
                "lrc_time": 20.27,
                "lrc_txt": "Psychic spies from China try to steal your mind's elation",
                "tokens": ["psychic", "spies", "from", "china", "try", "to", "steal", "your", "minds", "elation"],
                "content_tokens": ["psychic", "spies", "china", "try", "steal", "minds", "elation"],
                "informative_tokens": ["psychic", "spies", "china", "steal", "elation"],
            },
            {
                "lrc_time": 24.92,
                "lrc_txt": "And little girls from Sweden dream of silver-screen quotation",
                "tokens": ["and", "little", "girls", "from", "sweden", "dream", "of", "silver", "screen", "quotation"],
                "content_tokens": ["little", "girls", "sweden", "dream", "silver", "screen", "quotation"],
                "informative_tokens": ["little", "girls", "sweden", "silver", "quotation"],
            },
            {
                "lrc_time": 29.78,
                "lrc_txt": "And if you want these kind of dreams",
                "tokens": ["and", "if", "you", "want", "these", "kind", "of", "dreams"],
                "content_tokens": ["want", "these", "kind", "dreams"],
                "informative_tokens": ["want", "kind", "dreams"],
            },
        ]
        seg_rows = [
            {
                "st_abs": 23.0,
                "en_abs": 39.0,
                "anchor_abs": 23.0,
                "allow_line_aware_anchor": True,
                "tokens": [
                    "psychic",
                    "spies",
                    "from",
                    "china",
                    "try",
                    "to",
                    "steal",
                    "your",
                    "minds",
                    "elation",
                    "little",
                    "girls",
                    "from",
                    "sweden",
                    "dream",
                    "of",
                    "silver",
                    "screen",
                    "quotation",
                    "if",
                    "you",
                    "want",
                    "these",
                    "kind",
                    "of",
                    "dreams",
                    "its",
                    "californication",
                ],
                "content_tokens": [],
                "informative_tokens": [],
                "text": "combined",
            }
        ]

        offset_s, confidence = whisper_offset._estimate_offset_from_rows(
            window_rows=window_rows,
            seg_rows=seg_rows,
            max_abs_offset=90.0,
            return_confidence=True,
        )

        self.assertGreater(float(offset_s), 2.0)
        self.assertLess(float(offset_s), 6.0)
        self.assertGreater(float(confidence), 0.4)

    def test_build_segment_rows_enables_line_aware_anchor_by_default_for_early_lyrics(self) -> None:
        seg_rows = whisper_offset._build_segment_rows(
            [(10.0, 24.0, "line one line two line three")],
            frequent_tokens=set(),
            first_lrc_time=12.67,
        )

        self.assertEqual(len(seg_rows), 1)
        self.assertTrue(bool(seg_rows[0]["allow_line_aware_anchor"]))

    def test_build_segment_rows_respects_line_aware_threshold_override(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"KARAOKE_AUTO_OFFSET_LINE_AWARE_MIN_FIRST_LYRIC_SECS": "18.0"},
            clear=False,
        ):
            seg_rows = whisper_offset._build_segment_rows(
                [(10.0, 24.0, "line one line two line three")],
                frequent_tokens=set(),
                first_lrc_time=12.67,
            )

        self.assertEqual(len(seg_rows), 1)
        self.assertFalse(bool(seg_rows[0]["allow_line_aware_anchor"]))

    def test_find_whispercpp_bin_uses_related_git_worktrees(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            current_root = base / "current"
            sibling_root = base / "primary"
            (current_root / "scripts").mkdir(parents=True, exist_ok=True)
            bin_path = sibling_root / "whisper.cpp" / "build" / "bin" / "whisper-cli"
            bin_path.parent.mkdir(parents=True, exist_ok=True)
            bin_path.write_text("", encoding="utf-8")

            worktree_stdout = f"worktree {current_root}\nHEAD abc\n\nworktree {sibling_root}\nHEAD def\n"
            with (
                mock.patch.object(whisper_offset, "__file__", str(current_root / "scripts" / "lrc_offset_whisper.py")),
                mock.patch(
                    "scripts.lrc_offset_whisper.subprocess.run",
                    return_value=CompletedProcess(args=["git"], returncode=0, stdout=worktree_stdout, stderr=""),
                ),
                mock.patch("scripts.lrc_offset_whisper.shutil.which", return_value=None),
            ):
                got = whisper_offset._find_whispercpp_bin(None)

        self.assertEqual(got, str(bin_path))

    def test_find_model_uses_related_git_worktrees(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            current_root = base / "current"
            sibling_root = base / "primary"
            (current_root / "scripts").mkdir(parents=True, exist_ok=True)
            model_path = sibling_root / "whisper.cpp" / "models" / "ggml-base.bin"
            model_path.parent.mkdir(parents=True, exist_ok=True)
            model_path.write_text("", encoding="utf-8")

            worktree_stdout = f"worktree {current_root}\nHEAD abc\n\nworktree {sibling_root}\nHEAD def\n"
            with (
                mock.patch.object(whisper_offset, "__file__", str(current_root / "scripts" / "lrc_offset_whisper.py")),
                mock.patch(
                    "scripts.lrc_offset_whisper.subprocess.run",
                    return_value=CompletedProcess(args=["git"], returncode=0, stdout=worktree_stdout, stderr=""),
                ),
            ):
                got = whisper_offset._find_model(None)

        self.assertEqual(got, str(model_path))


if __name__ == "__main__":
    unittest.main()
