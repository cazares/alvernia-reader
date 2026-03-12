from pathlib import Path
import unittest


class ValidationSongListsTests(unittest.TestCase):
    def _load_queries(self, rel_path: str) -> list[str]:
        repo_root = Path(__file__).resolve().parents[1]
        path = repo_root / rel_path
        self.assertTrue(path.exists(), msg=f"missing validation list: {path}")
        out: list[str] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if (not s) or s.startswith("#"):
                continue
            out.append(s)
        return out

    def test_validation_lists_exclude_requested_instrumentals(self) -> None:
        updated = self._load_queries("tests/validation/updated_list_queries.txt")
        whisper = self._load_queries("tests/validation/whisper_alignment_queries.txt")
        merged = [q.lower() for q in (updated + whisper)]

        self.assertNotIn("metallica anesthesia pulling teeth instrumental", merged)
        self.assertNotIn("red hot chili peppers pretty little ditty instrumental", merged)

    def test_updated_list_applies_known_successful_query_rewrites(self) -> None:
        updated = [q.lower() for q in self._load_queries("tests/validation/updated_list_queries.txt")]
        self.assertIn("nirvana on a plain unplugged", updated)
        self.assertIn("shakira loca spanish version", updated)
        self.assertIn("shakira pies descalzos suenos blancos", updated)

