"""Runs the JavaScript tests of the edge router (grid_router_test.html) in a headless Chrome or Edge.

The test is skipped if no such browser is installed.
"""

import html
import re
import shutil
import subprocess
from pathlib import Path

import pytest

TEST_PAGE = Path(__file__).parent / "grid_router_test.html"

_BROWSERS = [
    "chrome",
    "google-chrome",
    "chromium",
    "chromium-browser",
    "msedge",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]


def _find_browser() -> str | None:
    for candidate in _BROWSERS:
        found = shutil.which(candidate) or (candidate if Path(candidate).is_file() else None)
        if found:
            return found
    return None


def test_grid_router():
    browser = _find_browser()
    if browser is None:
        pytest.skip("no Chrome/Edge found to run the JavaScript tests")
    dom = subprocess.run(
        [browser, "--headless=new", "--disable-gpu", "--virtual-time-budget=5000", "--dump-dom", TEST_PAGE.as_uri()],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    ).stdout
    title = re.search(r"<title>(.*?)</title>", dom)
    report = re.search(r'<pre id="out">(.*?)</pre>', dom, re.DOTALL)
    details = html.unescape(report.group(1)) if report else dom[-2000:]
    assert title, details
    assert title.group(1).startswith("PASS"), details
