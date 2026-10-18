import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import proxy_stop


class ProxyStopTest(unittest.TestCase):
    def test_departed_main_does_not_skip_waiting_for_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            group = Path(directory)
            (group / 'cgroup.procs').write_text('1073741824\n')
            output = io.StringIO()
            # The existing PID is absent from this simulated service. It must
            # not be signaled, nor cause the helper to abandon its bounded wait.
            with mock.patch.object(proxy_stop, 'unit_cgroup', return_value=group), \
                    mock.patch.dict(os.environ, MAINPID=str(os.getpid())), \
                    contextlib.redirect_stdout(output):
                self.assertEqual(proxy_stop.stop(.03), 1)
            result = json.loads(output.getvalue())
            self.assertEqual(result['proxy_stop'], 'timed_out')
            self.assertEqual(result['remaining'], [1073741824])
            self.assertGreaterEqual(result['elapsed_seconds'], .03)

    def test_recursive_membership_excludes_only_the_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            group = Path(directory)
            (group / '.control').mkdir()
            (group / 'cgroup.procs').write_text('1073741824\n')
            (group / '.control/cgroup.procs').write_text(f'{os.getpid()}\n1073741825\n')
            self.assertEqual(proxy_stop.processes(group), {1073741824, 1073741825})

    def test_wait_must_be_positive_and_finite(self):
        for value in (0, -1, float('nan'), float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                proxy_stop.stop(value)


if __name__ == '__main__':
    unittest.main()
