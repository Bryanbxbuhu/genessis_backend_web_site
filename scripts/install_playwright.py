"""Install Chromium, retrying transient browser/OS-package download failures."""

import argparse
import subprocess
import sys
import time


def install_chromium(*, with_deps=False, attempts=3):
    command = [sys.executable, "-m", "playwright", "install"]
    if with_deps:
        command.append("--with-deps")
    command.append("chromium")
    for attempt in range(1, attempts + 1):
        result = subprocess.run(command)
        if result.returncode == 0:
            return 0
        if attempt == attempts:
            return result.returncode
        delay = 15 * attempt
        print(f"Chromium installation failed (attempt {attempt}/{attempts}); retrying in {delay}s.", flush=True)
        time.sleep(delay)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--with-deps", action="store_true")
    args = parser.parse_args()
    raise SystemExit(install_chromium(with_deps=args.with_deps))
