import tempfile
import unittest
from pathlib import Path

from scripts import lrc_utils


class LrcUtilsTests(unittest.TestCase):
    def test_frac_to_secs_parses_1_2_3_digits(self) -> None:
        self.assertEqual(lrc_utils._frac_to_secs(None), 0.0)
        self.assertEqual(lrc_utils._frac_to_secs("1"), 0.1)
        self.assertEqual(lrc_utils._frac_to_secs("12"), 0.12)
        self.assertEqual(lrc_utils._frac_to_secs("123"), 0.123)
        self.assertEqual(lrc_utils._frac_to_secs("1234"), 0.123)

    def test_parse_lrc_from_text_extracts_meta_and_events(self) -> None:
        text = (
            "[ar:Artist]\n"
            "[ti:Title]\n"
            "[00:01.00]Hello\n"
            "[00:02.50][00:03.00]World\n"
        )
        events, meta = lrc_utils.parse_lrc(text)
        self.assertEqual(meta["ar"], "Artist")
        self.assertEqual(meta["ti"], "Title")
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].text, "Hello")
        self.assertAlmostEqual(events[1].t, 2.5)
        self.assertAlmostEqual(events[2].t, 3.0)

    def test_parse_lrc_from_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "song.lrc"
            p.write_text("[00:01.20]Line\n", encoding="utf-8")
            events, meta = lrc_utils.parse_lrc(str(p))
        self.assertEqual(meta, {})
        self.assertEqual(len(events), 1)
        self.assertAlmostEqual(events[0].t, 1.2)
        self.assertEqual(events[0].text, "Line")

    def test_parse_lrc_ignores_non_timestamp_lines(self) -> None:
        text = "plain text\n[xx:notmeta]\n[00:01.00]Line\n"
        events, _meta = lrc_utils.parse_lrc(text)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].text, "Line")

    def test_apply_global_offset_clamps_to_zero(self) -> None:
        events = [lrc_utils.LrcEvent(t=0.5, text="a"), lrc_utils.LrcEvent(t=2.0, text="b")]
        out = lrc_utils.apply_global_offset(events, -1.0)
        self.assertEqual(out[0].t, 0.0)
        self.assertEqual(out[1].t, 1.0)

    def test_format_lrc_orders_meta_preferred_keys(self) -> None:
        events = [lrc_utils.LrcEvent(t=1.23, text="hello")]
        meta = {"zz": "last", "ti": "Title", "ar": "Artist"}
        text = lrc_utils.format_lrc(events, meta)
        lines = text.splitlines()
        self.assertEqual(lines[0], "[ar:Artist]")
        self.assertEqual(lines[1], "[ti:Title]")
        self.assertIn("[zz:last]", lines)
        self.assertIn("[00:01.23] hello", lines[-1])
        self.assertTrue(text.endswith("\n"))

    def test_normalize_text_for_match(self) -> None:
        self.assertEqual(lrc_utils.normalize_text_for_match(" Héllo!! 123 "), "h llo 123")
        self.assertEqual(lrc_utils.normalize_text_for_match("A---B"), "a b")

    def test_tokens(self) -> None:
        self.assertEqual(lrc_utils.tokens("A b  C"), ["a", "b", "c"])
        self.assertEqual(lrc_utils.tokens("   "), [])


if __name__ == "__main__":
    unittest.main()
