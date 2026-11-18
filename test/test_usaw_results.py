"""
Tests for the USA Wrestling results parser.

Run with: uv run python test/test_usaw_results.py
"""

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from code import scrape_usaw_results as usaw  # noqa: E402
from code.scrape_usaw_results import (  # noqa: E402
    _parse_title,
    build_match_rows,
    build_placement_rows,
    parse_usaw_result,
)


CASES = [
    (
        "Fall with time",
        "Alex Smith (Prince William Wrestling Club) Fall 1:29 Ben Jones (King George Wrestling Club)",
        {"winner_name": "Alex Smith", "winner_team": "Prince William County", "loser_team": "King George",
         "decision_type": "fall", "decision_type_code": "Fall", "fall_time": "1:29", "winner_points": None},
    ),
    (
        "Major decision score",
        "Alex Smith (Bandits) Maj 15-3 Ben Jones (Annandale Mat Rats)",
        {"decision_type": "major decision", "decision_type_code": "Maj", "winner_points": 15,
         "loser_points": 3, "loser_team": "Annandale"},
    ),
    (
        "Tech fall with time in parentheses",
        "Alex Smith (Bandits) TF 10-1 (2:04) Ben Jones (Legacy Dragons)",
        {"decision_type": "tech fall", "winner_points": 10, "loser_points": 1, "fall_time": None,
         "loser_name": "Ben Jones"},
    ),
    (
        "Sudden victory",
        "Alex Smith (Bandits) SV-1 3-0 Ben Jones (Legacy Dragons)",
        {"decision_type": "sudden victory", "decision_type_code": "SV-1", "winner_points": 3},
    ),
    (
        "Numbered overtime",
        "Alex Smith (Bandits) 2-OT 3-3 Ben Jones (Legacy Dragons)",
        {"decision_type": "overtime", "decision_type_code": "2-OT", "winner_points": 3, "loser_points": 3},
    ),
    (
        "Injury default keeps time",
        "Alex Smith (Bandits) Inj. 1:40 Ben Jones (Legacy Dragons)",
        {"decision_type": "injury default", "decision_type_code": "Inj.", "fall_time": "1:40"},
    ),
    (
        "Forfeit without score",
        "Alex Smith (Bandits) FF Ben Jones (Legacy Dragons)",
        {"decision_type": "forfeit", "decision_type_code": "FF", "bye": False},
    ),
    (
        "Medical forfeit variant",
        "Alex Smith (Bandits) MFFL Ben Jones (Legacy Dragons)",
        {"decision_type": "medical forfeit", "decision_type_code": "MFFL"},
    ),
    (
        "Lowercase loser name is title-cased",
        "Alex Smith (Bandits) Dec 5-0 ben jones jr. (Legacy Dragons)",
        {"loser_name": "Ben Jones Jr", "decision_type": "decision"},
    ),
    (
        "Nickname and nested team parentheses",
        "Alex (AJ) Smith (Bandits (VA)) Dec 4-2 Ben Jones (Legacy Dragons)",
        {"winner_name": "Alex (Aj) Smith", "winner_team": "Bandits (VA)", "loser_name": "Ben Jones"},
    ),
    (
        "Double forfeit is excluded from Elo",
        "Alex Smith (Bandits) DFF Ben Jones (Legacy Dragons)",
        {"decision_type_code": "DFF", "bye": True},
    ),
    (
        "Unknown format is left unparsed",
        "Alex Smith (Bandits) XYZ Ben Jones (Legacy Dragons)",
        {"winner_name": None, "loser_name": None, "decision_type": None},
    ),
    (
        "Parenthesized fall",
        "Alex Smith (Shenendehowa) (Fall 0:28) Ben Jones (Niskayuna)",
        {"winner_team": "Shenendehowa", "loser_name": "Ben Jones", "decision_type": "fall",
         "decision_type_code": "Fall", "fall_time": "0:28"},
    ),
    (
        "Parenthesized tech fall keeps score, not time",
        "Alex Smith (Shenendehowa) (TF 15-0 3:12) Ben Jones (Niskayuna)",
        {"decision_type": "tech fall", "decision_type_code": "TF", "winner_points": 15,
         "loser_points": 0, "fall_time": None},
    ),
    (
        "Parenthesized TF-1.5 with time and nested score",
        "Alex Smith (Shenendehowa) (TF-1.5 4:00 (17-2)) Ben Jones (Niskayuna)",
        {"decision_type": "tech fall", "decision_type_code": "TF-1.5", "winner_points": 17,
         "loser_points": 2, "fall_time": "4:00"},
    ),
    (
        "Parenthesized forfeit codes kept verbatim",
        "Alex Smith (Shenendehowa) (M. For.) Ben Jones (Niskayuna)",
        {"decision_type": "medical forfeit", "decision_type_code": "M. For."},
    ),
    (
        "Parenthesized injury default without time",
        "Alex Smith (Shenendehowa) (Inj. [time]) Ben Jones (Niskayuna)",
        {"decision_type": "injury default", "decision_type_code": "Inj.", "fall_time": None},
    ),
]


class PlacementBackfillTests(unittest.TestCase):
    def test_existing_trackwrestling_event(self):
        for discovery in (False, True):
            for dry_run in (False, True):
                with self.subTest(discovery=discovery, dry_run=dry_run):
                    with patch.multiple(
                        usaw,
                        UsawClient=DEFAULT,
                        get_pg_connection=DEFAULT,
                        ensure_schema=DEFAULT,
                        upsert_governing_body=DEFAULT,
                        find_existing_event=DEFAULT,
                        compare_with_existing=DEFAULT,
                        has_placements=DEFAULT,
                        save_placements=DEFAULT,
                        save_event=DEFAULT,
                    ) as mocks:
                        client = mocks["UsawClient"].return_value
                        client.iter_index.return_value = iter([
                            usaw.UsawEvent(123, "Example Open", date(2026, 1, 1)),
                        ])
                        client.fetch_event.return_value = (
                            "Example Open - 01/01/2026 Results",
                            [["8U", "40", "Final", "Alex Smith (Bandits) Dec 4-2 Ben Jones (Bandits)"]],
                        )
                        client.fetch_placements.return_value = [
                            ["8U", "1", "40", "Alex Smith (Bandits)"],
                        ]
                        mocks["find_existing_event"].return_value = "tw-456"
                        mocks["compare_with_existing"].return_value = (0, 0)
                        mocks["has_placements"].return_value = False

                        failures = usaw.run(
                            name_pattern="Example" if discovery else None,
                            since=None,
                            event_ids=[] if discovery else [123],
                            dry_run=dry_run,
                            replace=False,
                        )

                        self.assertEqual(failures, 0)
                        client.fetch_placements.assert_called_once_with(123)
                        mocks["save_event"].assert_not_called()
                        if dry_run:
                            mocks["save_placements"].assert_not_called()
                            mocks["ensure_schema"].assert_not_called()
                        else:
                            save = mocks["save_placements"]
                            save.assert_called_once()
                            self.assertEqual(save.call_args.args[1], "tw-456")
                            self.assertEqual(save.call_args.args[2][0]["event_id"], "tw-456")

    def test_existing_placements_are_not_duplicated(self):
        client = MagicMock()
        with patch.object(usaw, "has_placements", return_value=True) as has, \
                patch.object(usaw, "save_placements") as save:
            result = usaw.backfill_placements(
                client, MagicMock(), usaw.UsawEvent(123, "Example", date(2026, 1, 1)),
                False, target_event_id="tw-456",
            )
            self.assertEqual(result, 0)
            self.assertEqual(has.call_args.args[1], "tw-456")
            client.fetch_placements.assert_not_called()
            save.assert_not_called()

    def test_no_posted_placements_does_not_write(self):
        client = MagicMock()
        client.fetch_placements.return_value = []
        with patch.object(usaw, "has_placements", return_value=False), \
                patch.object(usaw, "save_placements") as save:
            result = usaw.backfill_placements(
                client, MagicMock(), usaw.UsawEvent(123, "Example", date(2026, 1, 1)),
                False, target_event_id="tw-456",
            )
            self.assertEqual(result, 0)
            save.assert_not_called()


def main() -> int:
    failures = 0
    for name, text, expected in CASES:
        got = parse_usaw_result(text)
        diffs = {k: (v, got.get(k)) for k, v in expected.items() if got.get(k) != v}
        if diffs:
            failures += 1
            print(f"FAIL {name}: {diffs}")
        else:
            print(f"PASS {name}")

    title = _parse_title("NVWF 1.11.26 Hayfield Ban/Mids - 01/11/2026 Results")
    if title != ("NVWF 1.11.26 Hayfield Ban/Mids", __import__("datetime").date(2026, 1, 11)):
        failures += 1
        print(f"FAIL title parsing: {title}")
    else:
        print("PASS title parsing")

    rows = build_match_rows("e", [
        ["Division", "120 Champ. Round 2", "", "x"],
        ["Varsity", "107", "Quarterfinal", "x"],
    ])
    got = [(r["weight_class"], r["round_detail"]) for r in rows]
    if got != [("Division 120", "Champ. Round 2"), ("Varsity 107", "Quarterfinal")]:
        failures += 1
        print(f"FAIL round split from weight cell: {got}")
    else:
        print("PASS round split from weight cell")

    placements = build_placement_rows("e", [
        ["8U", "2", "40", "crew  setters (SK elite)"],
        ["8U", "1", "40", "Parker Simmons (Shamrock Wrestling Club)"],
        ["8U", "4", "43", "Justice Upchurch (Somerset Wrestling Club )"],
        ["12U", "1", "110-115", "Alex (AJ) Smith (Bandits (VA))"],
        ["12U", "DQ", "110-115", "No Team"],
    ])
    got = [(p["weight_class"], p["place"], p["name"], p["team"]) for p in placements]
    expected = [
        ("12U 110-115", 1, "Alex (Aj) Smith", "Bandits (VA)"),
        ("12U 110-115", None, "No Team", None),
        ("8U 40", 1, "Parker Simmons", "Shamrock"),
        ("8U 40", 2, "Crew Setters", "SK elite"),
        ("8U 43", 4, "Justice Upchurch", "Somerset"),
    ]
    if got != expected:
        failures += 1
        print(f"FAIL placement rows: {got}")
    else:
        print("PASS placement rows")

    import code.scrape_usaw_results as usaw

    class _Client:
        def fetch_placements(self, usaw_id):
            return [["8U", "1", "40", "Parker Simmons (Shamrock Wrestling Club)"]]

    saved = []
    orig = usaw.has_placements, usaw.save_placements
    usaw.has_placements = lambda conn, event_id: False
    usaw.save_placements = lambda conn, event_id, rows: saved.append((event_id, [r["event_id"] for r in rows]))
    try:
        usaw.backfill_placements(_Client(), None, usaw.UsawEvent(19991, "x", None), False, target_event_id="946889132")
    finally:
        usaw.has_placements, usaw.save_placements = orig
    if saved != [("946889132", ["946889132"])]:
        failures += 1
        print(f"FAIL placements saved under existing TrackWrestling event: {saved}")
    else:
        print("PASS placements saved under existing TrackWrestling event")

    print(f"\n{len(CASES) + 4 - failures}/{len(CASES) + 4} passed")
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(PlacementBackfillTests)
    )
    return 1 if failures or not result.wasSuccessful() else 0


if __name__ == "__main__":
    sys.exit(main())
