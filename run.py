from __future__ import annotations

import argparse
import os
import sys
import webbrowser
from pathlib import Path


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from preipo_arbitrage import REPORT, build


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新盘前股权合约套利看板")
    parser.add_argument("--no-open", action="store_true", help="生成后不自动打开浏览器")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build()
    if not args.no_open:
        if hasattr(os, "startfile"):
            os.startfile(str(REPORT))
        else:
            webbrowser.open(REPORT.as_uri())
    print({"dashboard": str(REPORT)})


if __name__ == "__main__":
    main()
