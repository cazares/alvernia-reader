import os
import unittest


class IntentionalGateFailureTests(unittest.TestCase):
    def test_intentionally_fails_for_gate_smoke_test(self) -> None:
        enabled = (
            os.environ.get("RUN_INTENTIONAL_GATE_FAILURE", "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            or os.environ.get("GATE_SMOKE_FAIL", "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        )
        if not enabled:
            self.skipTest(
                "Set RUN_INTENTIONAL_GATE_FAILURE=1 (or GATE_SMOKE_FAIL=1) to enable this gate smoke test."
            )
        self.assertEqual(1, 2, "Intentional failure to verify branch protection gate")


if __name__ == "__main__":
    unittest.main()
