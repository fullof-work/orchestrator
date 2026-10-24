"""Provisioning follows the prepared sandbox product without masking failures."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HELPER = Path(__file__).with_name('diff_fixture.sh')


class DiffFixture(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.output = self.root / 'new.diff'
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        binary = self.bin / 'sandbox-ctl'
        binary.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
if sys.argv[1:] == ['--help']:
    print('sandbox-ctl diff create' if os.environ['CURRENT'] == '1' else 'sandbox-ctl run')
    sys.exit(int(os.environ.get('HELP_ERROR', '0')))
pathlib.Path(os.environ['CALLS']).write_text(' '.join(sys.argv[1:]))
if os.environ.get('CREATE_ERROR'): sys.exit(19)
pathlib.Path(sys.argv[sys.argv.index('--output')+1]).write_bytes(b'current-format')
''')
        binary.chmod(0o755)

    def create(self, current=True, **overrides):
        environment = dict(os.environ, BIN=str(self.bin), CURRENT=str(int(current)),
                           CALLS=str(self.root/'calls'), FORMATS=str(self.root/'formats'), **overrides)
        return subprocess.run(['bash', '-c', '''set -euo pipefail
. "$1"
mkfs.ext4() { printf '%s\\n' "$*" > "$FORMATS"; }
create_ext4_fixture "$2" 8M
''', '_', str(HELPER), str(self.output)], env=environment, capture_output=True, text=True)

    def test_current_cli_creates_format(self):
        result = self.create()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_bytes(), b'current-format')
        self.assertEqual((self.root/'calls').read_text(),
                         f'diff create --output {self.output} --size 8M --filesystem ext4')
        self.assertFalse((self.root/'formats').exists())

    def test_historical_binary_gets_raw_ext4(self):
        result = self.create(current=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.stat().st_size, 8<<20)
        self.assertEqual(self.output.stat().st_blocks, 0)
        self.assertIn('-O ^has_journal', (self.root/'formats').read_text())

    def test_creation_failure_never_falls_back_to_raw(self):
        self.assertEqual(self.create(CREATE_ERROR='1').returncode, 19)
        self.assertFalse(self.output.exists())
        self.assertFalse((self.root/'formats').exists())

    def test_help_failure_is_not_a_legacy_product(self):
        self.assertEqual(self.create(HELP_ERROR='23').returncode, 23)
        self.assertFalse(self.output.exists())

    def test_existing_files_and_symlinks_are_preserved(self):
        self.output.write_bytes(b'existing')
        self.assertNotEqual(self.create().returncode, 0)
        self.assertEqual(self.output.read_bytes(), b'existing')
        self.output.unlink()
        target = self.root/'target'
        self.output.symlink_to(target)
        self.assertNotEqual(self.create().returncode, 0)
        self.assertTrue(self.output.is_symlink())
        self.assertFalse(target.exists())
