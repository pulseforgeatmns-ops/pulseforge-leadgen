#!/usr/bin/env python3
"""Linux-only local launcher. No downloads, credentials handling or shell eval."""
import ctypes
import os
import signal
import sys
if sys.platform != 'linux' or len(sys.argv) != 4:
    sys.exit(78)
expected_parent = int(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
# PR_SET_PDEATHSIG. Set before checking parent, closing parent-death race.
if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
    sys.exit(78)
if os.getppid() != expected_parent:
    sys.exit(78)
os.execv(sys.argv[2], [sys.argv[2], sys.argv[3]])
