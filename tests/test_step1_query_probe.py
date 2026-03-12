import tempfile
import unittest
from pathlib import Path

from scripts import step1_query_probe as probe


class Step1QueryProbeTests(unittest.TestCase):
    def test_strip_ansi_removes_escape_codes(self) -> None:
        raw = "\x1b[96mhello\x1b[0m world"
        self.assertEqual(probe._strip_ansi(raw), "hello world")

    def test_parse_timings_extracts_latest_part_values(self) -> None:
        text = """
[12:00:00] [TIMING] step=step1 part=fetch_lyrics elapsed_ms=101.2
[12:00:01] [TIMING] step=step1 part=download_audio elapsed_ms=55.4
[12:00:02] [TIMING] step=step1 part=fetch_lyrics elapsed_ms=202.5
[12:00:03] [TIMING] step=step2 part=total elapsed_ms=999.0
[12:00:04] [TIMING] step=step1 part=total elapsed_ms=300.0
"""
        got = probe._parse_timings(text)
        self.assertEqual(got["fetch_lyrics"], 202.5)
        self.assertEqual(got["download_audio"], 55.4)
        self.assertEqual(got["total"], 300.0)
        self.assertNotIn("step2.total", got)

    def test_extract_error_prefers_runtime_error_line(self) -> None:
        text = """
Traceback (most recent call last):
  File "x", line 1, in <module>
RuntimeError: No synced lyrics found for query: 'let it be'
"""
        got = probe._extract_error(text)
        self.assertEqual(got, "No synced lyrics found for query: 'let it be'")

    def test_extract_error_falls_back_to_error_tag(self) -> None:
        text = "[13:50:22] [ERROR] step1 failed due to timeout"
        self.assertEqual(probe._extract_error(text), "step1 failed due to timeout")

    def test_classify_error_categories(self) -> None:
        self.assertEqual(probe._classify_error("No synced lyrics found for query"), "lyrics_missing")
        self.assertEqual(probe._classify_error("No audio found for query"), "audio_missing")
        self.assertEqual(probe._classify_error("Read timed out"), "timeout")
        self.assertEqual(probe._classify_error("Sign in to confirm you're not a bot"), "bot_or_auth")
        self.assertEqual(probe._classify_error("yt-dlp download failed"), "download_error")
        self.assertEqual(probe._classify_error("ModuleNotFoundError: No module named requests"), "dependency_missing")
        self.assertEqual(probe._classify_error("unknown boom"), "other")
        self.assertEqual(probe._classify_error(""), "")

    def test_load_queries_dedupes_inline_and_file_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            query_file = Path(td) / "queries.txt"
            query_file.write_text(
                "\n".join(
                    [
                        "# comment",
                        "let it be",
                        "",
                        "john frusciante god",
                        "let it be",
                    ]
                ),
                encoding="utf-8",
            )

            got = probe._load_queries(query_file, ["let it be", "the beatles let it be"])
            self.assertEqual(
                got,
                [
                    "let it be",
                    "the beatles let it be",
                    "john frusciante god",
                ],
            )

    def test_coerce_text_handles_bytes(self) -> None:
        self.assertEqual(probe._coerce_text(b"hello"), "hello")
        self.assertEqual(probe._coerce_text("world"), "world")
        self.assertEqual(probe._coerce_text(None), "")


if __name__ == "__main__":
    unittest.main()
