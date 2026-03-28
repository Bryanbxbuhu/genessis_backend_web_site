#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Refresh global travel advisories only.

This script initializes the configured datastore and runs the
travel advisory sync path used by report generation.
"""

import sys
from pathlib import Path

# Fix Windows console encoding issues
if sys.platform == "win32" and hasattr(sys.stdout, "buffer"):
    import codecs

    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage import get_datastore
from scripts.sync_supabase import sync_global_advisories


def main() -> int:
    print("Refreshing travel advisories...")
    try:
        datastore = get_datastore()
    except Exception as exc:
        print(f"WARN Failed to initialize datastore: {exc}")
        return 0

    try:
        sync_global_advisories(datastore)
        print("Travel advisory refresh complete")
    except Exception as exc:
        print(f"WARN Travel advisory refresh failed: {exc}")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
