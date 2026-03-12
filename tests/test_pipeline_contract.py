import unittest

from scripts.pipeline_contract import build_pipeline_argv


class PipelineContractTests(unittest.TestCase):
    def test_audio_url_takes_precedence_over_audio_id(self) -> None:
        argv = build_pipeline_argv(
            query="Artist - Title",
            options={
                "audio_url": "https://placeholder.invalid/audio.m4a",
                "audio_id": "dQw4w9WgXcQ",
            },
        )
        self.assertIn("--audio-url", argv)
        self.assertIn("https://placeholder.invalid/audio.m4a", argv)
        self.assertNotIn("--audio-id", argv)

    def test_server_download_only_strips_audio_overrides(self) -> None:
        argv = build_pipeline_argv(
            query="Artist - Title",
            options={
                "audio_url": "https://placeholder.invalid/audio.m4a",
                "audio_id": "dQw4w9WgXcQ",
            },
            server_download_only=True,
        )
        self.assertNotIn("--audio-url", argv)
        self.assertNotIn("--audio-id", argv)

    def test_stem_flags_are_gated_by_allow_stem_levels(self) -> None:
        options = {"vocals": 80, "bass": 90, "drums": 100, "other": 70}

        with_stems = build_pipeline_argv(query="Artist - Title", options=options, allow_stem_levels=True)
        self.assertIn("--vocals", with_stems)
        self.assertIn("--bass", with_stems)
        self.assertIn("--drums", with_stems)
        self.assertIn("--other", with_stems)

        without_stems = build_pipeline_argv(query="Artist - Title", options=options, allow_stem_levels=False)
        self.assertNotIn("--vocals", without_stems)
        self.assertNotIn("--bass", without_stems)
        self.assertNotIn("--drums", without_stems)
        self.assertNotIn("--other", without_stems)

    def test_invalid_offset_is_ignored(self) -> None:
        argv = build_pipeline_argv(query="Artist - Title", options={"offset_sec": "not-a-number"})
        self.assertNotIn("--offset", argv)

    def test_maps_core_flags_to_cli_args(self) -> None:
        argv = build_pipeline_argv(
            query="Artist - Title",
            options={
                "force": True,
                "reset": True,
                "yt_search_n": 7,
                "retry_attempt": 2,
                "speed_mode": "EXTRA-TURBO",
                "skip_step1": True,
            },
        )
        self.assertIn("--force", argv)
        self.assertIn("--reset", argv)
        self.assertIn("--yt-search-n", argv)
        self.assertIn("7", argv)
        self.assertIn("--retry-attempt", argv)
        self.assertIn("2", argv)
        self.assertIn("--speed-mode", argv)
        self.assertIn("extra-turbo", argv)
        self.assertIn("--skip-step1", argv)

    def test_maps_auto_offset_controls_to_cli_args(self) -> None:
        argv = build_pipeline_argv(
            query="Artist - Title",
            options={
                "enable_auto_offset": 0,
                "calibration_level": 1,
            },
        )
        self.assertIn("--tune-for-me", argv)
        self.assertIn("--calibration-level", argv)
        self.assertIn("0", argv)
        self.assertIn("1", argv)


if __name__ == "__main__":
    unittest.main()
