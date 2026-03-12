from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _git_lines(*args: str) -> list[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in completed.stdout.splitlines() if line.strip()]


class RepoHygieneTests(unittest.TestCase):
    def test_no_tracked_files_with_duplicate_suffix_pattern(self) -> None:
        tracked = _git_lines("ls-files")
        offenders = [
            path
            for path in tracked
            if " 2." in Path(path).name
        ]
        self.assertEqual(
            offenders,
            [],
            f"tracked duplicate-suffix files found: {offenders}",
        )

    def test_no_tracked_gitlinks(self) -> None:
        staged_entries = _git_lines("ls-files", "--stage")
        gitlinks = []
        for line in staged_entries:
            # Format: "<mode> <sha> <stage>\t<path>"
            prefix, _, path = line.partition("\t")
            mode = prefix.split(" ", 1)[0].strip()
            if mode == "160000":
                gitlinks.append(path)
        self.assertEqual(gitlinks, [], f"tracked gitlinks found: {gitlinks}")


if __name__ == "__main__":
    unittest.main()
