import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import n1_ysld as n1  # noqa: E402


def fixture(ending=b"\n", final_newline=True):
    args = ["value"]
    for index in range(256):
        args.append("'#%06X'" % (index % 32))  # Repeated colors are valid palette data.
        if index < 255:
            coefficient = f"{(index + 1) / 256:.8f}".rstrip("0").rstrip(".")
            args.append(f"{coefficient}*(inputmax-inputmin)+inputmin")
    out = ["title: synthetic", "rules:"]
    for property_name in ("fill", "stroke", "label", "mark"):
        out.append(f"  {property_name}: ${{categorize(value, ")
        remaining_args = args[1:]
        for start in range(0, len(remaining_args), 8):
            chunk = remaining_args[start:start + 8]
            tail = start + len(chunk) == len(remaining_args)
            out.append("    " + ", ".join(chunk) + (")} " if tail else ","))
    out.append("  opacity: 0")
    data = ending.join(line.encode("ascii") for line in out)
    return data + (ending if final_newline else b"")


class N1YSLDTests(unittest.TestCase):
    def setUp(self):
        self.original = fixture()
        self.candidate = n1.make_bytes(self.original)

    def test_transform_and_independent_static_check(self):
        facts, failures = n1.check_bytes(self.original, self.candidate)
        self.assertEqual(failures, [])
        self.assertTrue(facts["lines_outside_expressions_unchanged"])
        self.assertTrue(facts["color_sequence_unchanged"])
        self.assertEqual(n1.make_bytes(self.original), self.candidate)

    def test_crlf_and_missing_final_newline_are_preserved(self):
        for original in (fixture(b"\r\n"), fixture(b"\r\n", False), fixture(b"\n", False)):
            candidate = n1.make_bytes(original)
            self.assertEqual(n1.split_lines(original)[1:], n1.split_lines(candidate)[1:])
            self.assertEqual(n1.check_bytes(original, candidate)[1], [])

    def test_wrong_expression_count_is_rejected(self):
        altered = self.original.replace(b"  mark: ${categorize(", b"  mark: value # ${categorize(")
        with self.assertRaisesRegex(ValueError, "expected 4 categorize"):
            n1.make_bytes(altered)

    def test_wrong_source_threshold_is_rejected(self):
        altered = self.original.replace(b"0.00390625*(inputmax-inputmin)+inputmin", b"0.0039*(inputmax-inputmin)+inputmin", 1)
        with self.assertRaisesRegex(ValueError, "source rejected"):
            n1.make_bytes(altered)

    def test_different_source_expression_is_rejected(self):
        altered = self.original.replace(b"'#000001'", b"'#ABCDEF'", 1)
        with self.assertRaisesRegex(ValueError, "source rejected"):
            n1.make_bytes(altered)

    def test_unrecognized_categorize_occurrence_is_rejected(self):
        altered = self.original.replace(b"title: synthetic", b"title: synthetic # ${categorize(unreviewed)}")
        with self.assertRaisesRegex(ValueError, "unrecognized categorize"):
            n1.make_bytes(altered)

    def test_non_target_bytes_must_match_in_check(self):
        altered = self.candidate.replace(b"title: synthetic", b"title: changed")
        facts, failures = n1.check_bytes(self.original, altered)
        self.assertTrue(any("outside the four expressions" in item for item in failures))
        self.assertFalse(facts["lines_outside_expressions_unchanged"])

    def test_candidate_threshold_must_be_exact(self):
        altered = self.candidate.replace(b"'#000000', 0.00390625", b"'#000000', 0.0039", 1)
        self.assertTrue(n1.check_bytes(self.original, altered)[1])

    def test_candidate_must_preserve_every_source_color(self):
        altered = self.candidate.replace(b"'#000000', 0.00390625", b"'#ABCDEF', 0.00390625", 1)
        facts, failures = n1.check_bytes(self.original, altered)
        self.assertIn("color sequence changed", failures)
        self.assertFalse(facts["color_sequence_unchanged"])

    def test_line_count_failure_does_not_claim_outside_bytes_unchanged(self):
        altered = self.candidate + b"# extra line\n"
        facts, failures = n1.check_bytes(self.original, altered)
        self.assertTrue(failures)
        self.assertFalse(facts["lines_outside_expressions_unchanged"])

    def test_line_ending_failure_does_not_claim_outside_bytes_unchanged(self):
        altered = self.candidate.replace(b"\n", b"\r\n")
        facts, failures = n1.check_bytes(self.original, altered)
        self.assertTrue(any("line ending" in item for item in failures))
        self.assertFalse(facts["lines_outside_expressions_unchanged"])

    def test_mixed_line_endings_are_rejected(self):
        altered = self.original.replace(b"\n", b"\r\n", 1)
        with self.assertRaisesRegex(ValueError, "mixed line endings"):
            n1.make_bytes(altered)

    def test_output_is_private_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = pathlib.Path(temporary) / "out"
            out.mkdir(mode=0o700)
            out.chmod(0o700)
            path = pathlib.Path(n1.write_new(str(out), "style_n1.yaml", self.candidate))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_bytes(), self.candidate)
            with self.assertRaises(FileExistsError):
                n1.write_new(str(out), "style_n1.yaml", self.candidate)

    def test_partial_os_writes_are_completed(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = pathlib.Path(temporary) / "out"
            out.mkdir(mode=0o700)
            out.chmod(0o700)
            real_write = os.write

            def partial_write(fd, data):
                return real_write(fd, data[:17])

            with mock.patch.object(n1.os, "write", side_effect=partial_write):
                path = n1.write_new(str(out), "style_n1.yaml", self.candidate)
            self.assertEqual(pathlib.Path(path).read_bytes(), self.candidate)

    def test_output_directory_must_be_mode_0700(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = pathlib.Path(temporary) / "out"
            out.mkdir(mode=0o755)
            out.chmod(0o755)
            with self.assertRaisesRegex(ValueError, "mode 0700"):
                n1.write_new(str(out), "style_n1.yaml", self.candidate)

    def test_candidate_naming_preserves_reverse_suffix_convention(self):
        self.assertEqual(n1.candidate_filename("viridis_256.yaml"), "viridis_256_n1.yaml")
        self.assertEqual(n1.candidate_filename("ice_256_i.ysld"), "ice_256_n1_i.ysld")

    def test_cli_make_and_check_print_only_metadata(self):
        tool = pathlib.Path(n1.__file__)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "ice_256_i.ysld"
            source.write_bytes(self.original)
            out = root / "out"
            out.mkdir(mode=0o700)
            out.chmod(0o700)
            made = subprocess.run(
                [sys.executable, str(tool), "make", str(source), str(out)],
                check=True, capture_output=True, text=True,
            )
            candidate = out / "ice_256_n1_i.ysld"
            checked = subprocess.run(
                [sys.executable, str(tool), "check", str(source), str(candidate)],
                check=True, capture_output=True, text=True,
            )
            self.assertIn("ice_256_n1_i.ysld", made.stdout)
            self.assertIn("N1-STATIC-PASS", checked.stdout)
            self.assertNotIn("#000000", made.stdout + checked.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
