#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT))

from open_from_db_common import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main("bilibili"))
