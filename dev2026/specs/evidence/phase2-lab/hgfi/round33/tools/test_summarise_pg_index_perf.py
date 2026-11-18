import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROUND = HERE.parent
SCRIPT = HERE / "summarise_pg_index_perf.py"
PERF = ROUND / "expansion" / "perf.json"
EXPECTED = ROUND / "analysis" / "perf-by-layer.tsv"


class PublicSummaryTests(unittest.TestCase):
    def test_rebuilds_selected_round33_table_byte_for_byte(self):
        with tempfile.TemporaryDirectory() as tmp:
            actual = Path(tmp) / "perf-by-layer.tsv"
            result = subprocess.run([sys.executable, str(SCRIPT), str(PERF), str(actual)],
                                    check=True, capture_output=True, text=True)
            self.assertIn("PASS: 1344 timing rows, 12 layers", result.stdout)
            self.assertEqual(actual.read_bytes(), EXPECTED.read_bytes())

    def test_rejects_a_cell_missing_an_arm(self):
        from summarise_pg_index_perf import summarize

        rows = [{"cell": "x", "arm": "A", "execution_ms": 1, "layer": "L",
                 "family": "F", "regime": "annual", "grid": "1deg", "parameter": "v"}]
        with self.assertRaisesRegex(ValueError, "all four arms"):
            summarize(rows)


if __name__ == "__main__":
    unittest.main()
