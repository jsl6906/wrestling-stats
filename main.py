from __future__ import annotations

import argparse
from typing import Optional, List
import importlib.util
import pathlib
import sys


def _load_scraper_main():
    mod_path = pathlib.Path(__file__).parent / 'code' / 'scrape_raw_data.py'
    spec = importlib.util.spec_from_file_location("scrape_raw_data", str(mod_path))
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        # Ensure dataclasses can resolve __module__ via sys.modules
        sys.modules[spec.name] = mod  # type: ignore[index]
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        return mod.main
    raise RuntimeError("Failed to load scrape_raw_data module")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="wrestling-stats", description="NVWF scraping tools")
    sub = parser.add_subparsers(dest="cmd")

    scrape = sub.add_parser("scrape", help="Scrape TrackWrestling raw HTML")
    scrape.add_argument("--season", help="Optional season filter like 2024-2025 (informational)")
    scrape.add_argument("--max-tournaments", type=int, default=None)
    scrape.add_argument("--show", action="store_true")
    scrape.add_argument("--resume", action="store_true")

    args = parser.parse_args(argv)
    if args.cmd == "scrape":
        scrape_main = _load_scraper_main()
        return scrape_main([
            *( ["--season", args.season] if getattr(args, "season", None) else [] ),
            *( ["--max-tournaments", str(args.max_tournaments)] if args.max_tournaments else [] ),
            *( ["--show"] if args.show else [] ),
            *( ["--resume"] if args.resume else [] ),
        ])

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
