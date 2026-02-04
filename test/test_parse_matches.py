"""
Test suite for parse_match_text function.

Run with: uv run python test/test_parse_matches.py
"""

import sys
from pathlib import Path

# Add parent directory to path so we can import from code/
sys.path.insert(0, str(Path(__file__).parent.parent))

from code.parse_round_html import parse_match_text
from typing import Dict, Any, List
import json


class TestCase:
    def __init__(self, name: str, input_text: str, expected: Dict[str, Any]):
        self.name = name
        self.input_text = input_text
        self.expected = expected


# Define test cases with expected outputs
TEST_CASES = [
    TestCase(
        name="DFF (Double Forfeit)",
        input_text="Round 5 - Cooper Green (CATHOLIC) 2-3 and Daniel Hasbun (HICKORY) 2-3 (DFF)",
        expected={
            "round_detail": "Round 5",
            "winner_name": "Cooper Green",
            "winner_team": "CATHOLIC",
            "loser_name": "Daniel Hasbun",
            "loser_team": "HICKORY",
            "decision_type": "bye",
            "decision_type_code": "DFF",
            "bye": True,
        }
    ),
    
    TestCase(
        name="DFF with only one wrestler (should be bye)",
        input_text="Quarterfinal -   ()  and Kevin Ford (George Wythe) 1-1 (DFF)",
        expected={
            "round_detail": "Quarterfinal",
            "winner_name": "Kevin Ford",
            "winner_team": "George Wythe",
            "loser_name": None,
            "loser_team": None,
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "bye": True,
        }
    ),
    
    TestCase(
        name="Empty loser name (forfeit)",
        input_text="Champ. Round 1 - Aiden Blackwelder (Glen Allen) 9-6 won by forfeit over () (For.)",
        expected={
            "round_detail": "Champ. Round 1",
            "winner_name": "Aiden Blackwelder",
            "winner_team": "Glen Allen",
            "decision_type": "forfeit",
            "decision_type_code": "For.",
            "loser_name": None,
            "loser_team": None,
            "bye": False,
        }
    ),
    
    TestCase(
        name="Forfeit Forfeit loser name (should be bye)",
        input_text="Cons. Round 1 - Vernon Dudley (E. C. Glass) 2-7 won by forfeit over Forfeit Forfeit (Poquoson) 4-8 (For.)",
        expected={
            "round_detail": "Cons. Round 1",
            "winner_name": "Vernon Dudley",
            "winner_team": "E. C. Glass",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "loser_name": None,
            "loser_team": None,
            "bye": True,
        }
    ),
    
    TestCase(
        name="Forfeit Bye loser name (should be bye)",
        input_text="Quarterfinal - Taylor Misuna (Grassfield High School) 3-0 won by tech fall over Forfeit Bye (Hanover High School) 1-3 (TF-1.5 4:26 (18-3))",
        expected={
            "round_detail": "Quarterfinal",
            "winner_name": "Taylor Misuna",
            "winner_team": "Grassfield",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "loser_name": None,
            "loser_team": None,
            "bye": True,
        }
    ),
    
    TestCase(
        name="Medical Forfeit loser name (should be bye)",
        input_text="Cons. Round 4 - Natalie Zavala (Martinsville) 4-7 won by forfeit over Medical Forfeit (Rustburg) 7-3 (For.)",
        expected={
            "round_detail": "Cons. Round 4",
            "winner_name": "Natalie Zavala",
            "winner_team": "Martinsville",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "loser_name": None,
            "loser_team": None,
            "bye": True,
        }
    ),
    
    TestCase(
        name="Remove (FORFEIT) from loser name",
        input_text="Quarterfinal - Christopher Floyd (Martinsville) 14-14 won by forfeit over Devin Rader (FORFEIT) (Radford) 6-7 (For.)",
        expected={
            "round_detail": "Quarterfinal",
            "winner_name": "Christopher Floyd",
            "winner_team": "Martinsville",
            "decision_type": "forfeit",
            "loser_name": "Devin Rader",
            "loser_team": "Radford",
            "decision_type_code": "For.",
            "bye": False,
        }
    ),
    
    TestCase(
        name="Remove (NS) from winner name",
        input_text="Cons. Round 3 - Gavin Small (NS) (James Wood High School) 3-2 won by fall over Elber Fuentes (Liberty (Bealeton)) 1-2 (Fall 2:55)",
        expected={
            "round_detail": "Cons. Round 3",
            "winner_name": "Gavin Small",
            "winner_team": "James Wood",
            "decision_type": "fall",
            "loser_name": "Elber Fuentes",
            "loser_team": "Liberty (Bealeton)",
            "decision_type_code": "Fall",
            "fall_time": "2:55",
        }
    ),
    
    TestCase(
        name="Bye Bye loser name (should be bye)",
        input_text="Round 1 - Xavier Nieves (Colgan) 20-8 won by forfeit over Bye Bye (Tuscarora) 1-4 (For.)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Xavier Nieves",
            "winner_team": "Colgan",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "loser_name": None,
            "loser_team": None,
            "bye": True,
        }
    ),
    
    TestCase(
        name="'. bye' loser name with punctuation (should be bye)",
        input_text="Round 1 - Isaiah Hodel (James Wood) 30-4 won by forfeit over . bye () 0-4 (For.)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Isaiah Hodel",
            "winner_team": "James Wood",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "loser_name": None,
            "loser_team": None,
            "bye": True,
        }
    ),
    
    TestCase(
        name="Remove seed number (1) from winner name",
        input_text="Champ. Round 1 - Maima Dandai  (1) (Charlottesville High School) 1-3 won by forfeit over Kyelle Hicks (Armstrong High School) 0-2 (For.)",
        expected={
            "round_detail": "Champ. Round 1",
            "winner_name": "Maima Dandai",
            "winner_team": "Charlottesville",
            "decision_type": "forfeit",
            "loser_name": "Kyelle Hicks",
            "loser_team": "Armstrong",
            "decision_type_code": "For.",
            "bye": False,
        }
    ),
    
    TestCase(
        name="Both winner and loser empty (should be bye - no data)",
        input_text="2nd Place Match -   ()  won by no contest over   ()  (NC)",
        expected={
            "round_detail": "2nd Place Match",
            "winner_name": None,
            "winner_team": None,
            "decision_type": "bye",
            "loser_name": None,
            "loser_team": None,
            "decision_type_code": "Bye",
            "bye": True,
        }
    ),
    
    TestCase(
        name="'over' format with TB-2 (Fall) - plain text from HTML",
        input_text="Isaac Hegg  (Oakton)  over  Quincy Hinrichs (Warhill)  TB-2 (Fall) 0:48",
        expected={
            "winner_name": "Isaac Hegg",
            "winner_team": "Oakton",
            "decision_type": "fall",
            "loser_name": "Quincy Hinrichs",
            "loser_team": "Warhill",
            "decision_type_code": "TB-2",
            "fall_time": "0:48",
        }
    ),
    
    TestCase(
        name="Remove seed number prefix '11/' from winner name",
        input_text="3rd Place Match - 11/Dustin Tucker (Patrick County) 27-8 won by fall over Angel Santiago (Hidden Valley) 25-21 (Fall 2:24)",
        expected={
            "round_detail": "3rd Place Match",
            "winner_name": "Dustin Tucker",
            "winner_team": "Patrick County",
            "decision_type": "fall",
            "loser_name": "Angel Santiago",
            "loser_team": "Hidden Valley",
            "decision_type_code": "Fall",
            "fall_time": "2:24",
        }
    ),
    
    TestCase(
        name="Replace pound sign sequences with 'Unknown Wrestler'",
        input_text="Champ. Round 2 - ######### ######### (Mountain View) 9-7 won by fall over Tanner Kump (Patrick Henry – A) 10-12 (Fall 5:10)",
        expected={
            "round_detail": "Champ. Round 2",
            "winner_name": "Unknown Wrestler",
            "winner_team": "Mountain View",
            "decision_type": "fall",
            "loser_name": "Tanner Kump",
            "loser_team": "Patrick Henry – A",
            "decision_type_code": "Fall",
            "fall_time": "5:10",
        }
    ),
    
    TestCase(
        name="Remove leading parenthesis and handle nested nickname (RJ (Ranjit) Roberts)",
        input_text="Champ. Round 2 - Wilmer Bonilla Banegas (John Lewis) 7-7 won by fall over (RJ (Ranjit) Roberts (Oakton) 0-2 (Fall 1:14)",
        expected={
            "round_detail": "Champ. Round 2",
            "winner_name": "Wilmer Bonilla Banegas",
            "winner_team": "John Lewis",
            "decision_type": "fall",
            "loser_name": "RJ (Ranjit) Roberts",
            "loser_team": "Oakton",
            "decision_type_code": "Fall",
            "fall_time": "1:14",
        }
    ),
    
    TestCase(
        name="Replace ' - ' with '-' in names (Villa - Soto -> Villa-Soto)",
        input_text="Cons. Round 2 - Raul Villa - Soto (Clarke County) 11-9 won by fall over Will Pallela (Alleghany) 5-6 (Fall 2:45)",
        expected={
            "round_detail": "Cons. Round 2",
            "winner_name": "Raul Villa-Soto",
            "winner_team": "Clarke County",
            "decision_type": "fall",
            "loser_name": "Will Pallela",
            "loser_team": "Alleghany",
            "decision_type_code": "Fall",
            "fall_time": "2:45",
        }
    ),
    
    TestCase(
        name="Remove ' - *' suffix from names (Adam Preston - * -> Adam Preston)",
        input_text="Round 3 - Adam Preston - * (Franklin County) won by major decision over Brent Bowling (Franklin County) Maj 12-3",
        expected={
            "round_detail": "Round 3",
            "winner_name": "Adam Preston",
            "winner_team": "Franklin County",
            "decision_type": "major decision",
            "loser_name": "Brent Bowling",
            "loser_team": "Franklin County",
            "decision_type_code": "Maj",
            "winner_points": 12,
            "loser_points": 3,
        }
    ),
    
    TestCase(
        name="Normalize acronyms with spaces (C J -> CJ)",
        input_text="Round 1 - C J Smith (Team A) 5-0 won by decision over John Doe (Team B) 3-2 (Dec 7-3)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "CJ Smith",
            "winner_team": "Team A",
            "decision_type": "decision",
            "loser_name": "John Doe",
            "loser_team": "Team B",
            "decision_type_code": "Dec",
            "winner_points": 7,
            "loser_points": 3,
        }
    ),
    
    TestCase(
        name="Normalize acronyms with periods (C.J. -> CJ)",
        input_text="Round 2 - C.J. Williams (Team C) 4-1 won by fall over Jane Smith (Team D) 2-3 (Fall 1:23)",
        expected={
            "round_detail": "Round 2",
            "winner_name": "CJ Williams",
            "winner_team": "Team C",
            "decision_type": "fall",
            "loser_name": "Jane Smith",
            "loser_team": "Team D",
            "decision_type_code": "Fall",
            "fall_time": "1:23",
        }
    ),
    
    TestCase(
        name="Normalize acronyms with > characters (C>J> -> CJ)",
        input_text="Round 3 - C>J> Jones (Team E) 6-0 won by tech fall over Bob Lee (Team F) 1-4 (TF 15-0)",
        expected={
            "round_detail": "Round 3",
            "winner_name": "CJ Jones",
            "winner_team": "Team E",
            "decision_type": "tech fall",
            "loser_name": "Bob Lee",
            "loser_team": "Team F",
            "decision_type_code": "TF",
            "winner_points": 15,
            "loser_points": 0,
        }
    ),
    
    TestCase(
        name="Normalize multi-letter acronyms (A.J.K. -> AJK)",
        input_text="Round 1 - A.J.K. Rodriguez (Team G) 3-1 won by decision over Mike Davis (Team H) 2-2 (Dec 5-2)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "AJK Rodriguez",
            "winner_team": "Team G",
            "decision_type": "decision",
            "loser_name": "Mike Davis",
            "loser_team": "Team H",
            "decision_type_code": "Dec",
            "winner_points": 5,
            "loser_points": 2,
        }
    ),
    
    TestCase(
        name="Injury default decision type",
        input_text="Semifinal - Ryan Kaviani (Langley) won by injury default over Jack Flood (Yorktown) Inj. 4:45",
        expected={
            "round_detail": "Semifinal",
            "winner_name": "Ryan Kaviani",
            "winner_team": "Langley",
            "decision_type": "injury default",
            "loser_name": "Jack Flood",
            "loser_team": "Yorktown",
            "decision_type_code": "Inj.",
            "fall_time": "4:45",
        }
    ),
    
    TestCase(
        name="Double overtime (2-OT)",
        input_text="3rd Place Match - nana utsey (Glen Allen) 17-3 won in double overtime over Kenneth Hamilton (Gloucester) 16-3 (2-OT 7-5)",
        expected={
            "round_detail": "3rd Place Match",
            "winner_name": "Nana Utsey",
            "winner_team": "Glen Allen",
            "decision_type": "overtime",  # Normalized to "overtime"
            "loser_name": "Kenneth Hamilton",
            "loser_team": "Gloucester",
            "decision_type_code": "2-OT",
            "winner_points": 7,
            "loser_points": 5,
        }
    ),
    
    TestCase(
        name="Hyphen in name (Sampson - Johnson -> Sampson-Johnson)",
        input_text="Jamil Reyes (Osbourn) over Jadin Sampson - Johnson (Chancellor) Fall 3:34",
        expected={
            "winner_name": "Jamil Reyes",
            "winner_team": "Osbourn",
            "loser_name": "Jadin Sampson-Johnson",
            "loser_team": "Chancellor",
            "decision_type": "fall",
            "fall_time": "3:34",
        }
    ),
    
    TestCase(
        name="Nickname in parentheses",
        input_text="Cons. Round 2 - Bilegt (Billy) Arslan (Mclean ) 2-1 won by decision over Collin Carr (Heritage-Leesburg) 1-2 (Dec 4-0)",
        expected={
            "round_detail": "Cons. Round 2",
            "winner_name": "Bilegt (Billy) Arslan",
            "winner_team": "Mclean",
            "decision_type": "decision",
            "loser_name": "Collin Carr",
            "loser_team": "Heritage-Leesburg",
            "decision_type_code": "Dec",
            "winner_points": 4,
            "loser_points": 0,
        }
    ),
    
    TestCase(
        name="Won in X by Y format (SV-1 by fall)",
        input_text="Semifinal - Jax Engh (Culpeper County) 27-4 won in SV-1 by fall over Nathan Taylor (Hopewell) 19-3 (SV-1 (Fall) 6:30)",
        expected={
            "round_detail": "Semifinal",
            "winner_name": "Jax Engh",
            "winner_team": "Culpeper County",
            "decision_type": "fall",
            "loser_name": "Nathan Taylor",
            "loser_team": "Hopewell",
            "decision_type_code": "SV-1",
            "fall_time": "6:30",
        }
    ),
    
    TestCase(
        name="DDQ (Double Disqualification)",
        input_text="Cons. Round 2 - Aaron Hobbs (Norfolk Christian) 1-3 and Wiley Farrer (Hickory) 0-2 (DDQ)",
        expected={
            "winner_name": "Aaron Hobbs",
            "winner_team": "Norfolk Christian",
            "loser_name": "Wiley Farrer",
            "loser_team": "Hickory",
            "decision_type": "bye",
            "decision_type_code": "DDQ",
            "bye": True,
        }
    ),
    
    TestCase(
        name="Negative score adjustment",
        input_text="-3.0",
        expected={
            "decision_type": "bye",
            "decision_type_code": "SCORE",
            "bye": True,
        }
    ),
    
    TestCase(
        name="TB-3 riding time with score",
        input_text="1st Place Match - Caitlin Rankin (Riverbend) 14-1 won in TB-3 by riding time over Hayden Mayo (Western Branch) 3-1 (TB-3 (RT) 2-2)",
        expected={
            "round_detail": "1st Place Match",
            "winner_name": "Caitlin Rankin",
            "winner_team": "Riverbend",
            "decision_type": "riding time",
            "loser_name": "Hayden Mayo",
            "loser_team": "Western Branch",
            "decision_type_code": "TB-3",
            "winner_points": 2,
            "loser_points": 2,
        }
    ),
    
    TestCase(
        name="TB-2 with (Fall) detail - 'over' format",
        input_text="Matt McKim (Woodgrove) over Matthew Bourgoin (Warren County) TB-2 (Fall) 0:00",
        expected={
            "winner_name": "Matt Mckim",
            "winner_team": "Woodgrove",
            "decision_type": "fall",
            "loser_name": "Matthew Bourgoin",
            "loser_team": "Warren County",
            "decision_type_code": "TB-2",
            "fall_time": "0:00",
        }
    ),
    
    TestCase(
        name="Nickname in loser name - 'over' format",
        input_text="Angelo Norwood (Kellam HS) over John (Peyton) Cherkaur (Gloucester HS) Fall 2:28",
        expected={
            "winner_name": "Angelo Norwood",
            "winner_team": "Kellam",
            "decision_type": "fall",
            "loser_name": "John (Peyton) Cherkaur",
            "loser_team": "Gloucester",
            "decision_type_code": "Fall",
            "fall_time": "2:28",
        }
    ),
    
    TestCase(
        name="Empty loser with just parentheses",
        input_text="Champ. Round 1 - Tony Lattanze (Brentsville) won by forfeit over   () FF",
        expected={
            "round_detail": "Champ. Round 1",
            "winner_name": "Tony Lattanze",
            "winner_team": "Brentsville",
            "decision_type": "forfeit",
            "loser_name": None,
            "loser_team": None,
        }
    ),
    
    TestCase(
        name="Standard fall with time",
        input_text="Round 1 - John Smith (Team A) 5-0 won by fall over Jane Doe (Team B) 3-2 (Fall 2:15)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "John Smith",
            "winner_team": "Team A",
            "decision_type": "fall",
            "loser_name": "Jane Doe",
            "loser_team": "Team B",
            "decision_type_code": "Fall",
            "fall_time": "2:15",
        }
    ),
    
    TestCase(
        name="Major decision with score",
        input_text="Quarterfinal - Alice Johnson (Warriors) 10-2 won by major decision over Bob Wilson (Knights) 8-5 (MD 12-3)",
        expected={
            "round_detail": "Quarterfinal",
            "winner_name": "Alice Johnson",
            "winner_team": "Warriors",
            "decision_type": "major decision",
            "loser_name": "Bob Wilson",
            "loser_team": "Knights",
            "decision_type_code": "MD",
            "winner_points": 12,
            "loser_points": 3,
        }
    ),
    
    TestCase(
        name="Tech fall with nested score",
        input_text="Semifinal - Mike Davis (Eagles) 12-0 won by tech fall over Chris Lee (Tigers) 7-3 (TF-1.5 5:20 (16-0))",
        expected={
            "round_detail": "Semifinal",
            "winner_name": "Mike Davis",
            "winner_team": "Eagles",
            "decision_type": "tech fall",
            "loser_name": "Chris Lee",
            "loser_team": "Tigers",
            "decision_type_code": "TF-1.5",
            "winner_points": 16,
            "loser_points": 0,
        }
    ),
    
    TestCase(
        name="Received a bye",
        input_text="Round 2 - Sarah Miller (Panthers) 8-1 received a bye",
        expected={
            "round_detail": "Round 2",
            "winner_name": "Sarah Miller",
            "winner_team": "Panthers",
            "decision_type": "bye",
            "decision_type_code": "Bye",
            "bye": True,
        }
    ),
    
    TestCase(
        name="Name conversion: Mateo Corsini -> Matteo Corsini",
        input_text="Round 1 - Mateo Corsini (Team A) 5-0 won by decision over James Brown (Team B) 3-2 (Dec 7-2)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Matteo Corsini",
            "winner_team": "Team A",
            "decision_type": "decision",
            "loser_name": "James Brown",
            "loser_team": "Team B",
            "decision_type_code": "Dec",
            "winner_points": 7,
            "loser_points": 2,
        }
    ),
    
    TestCase(
        name="Nested parentheses in team name",
        input_text="Round 1 - Ahmed Ghandour (Freedom (South Riding)) 7-4 won by fall over Zachary Abdo (Potomac Falls) 2-4 (Fall 1:23)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Ahmed Ghandour",
            "winner_team": "Freedom (South Riding)",
            "decision_type": "fall",
            "loser_name": "Zachary Abdo",
            "loser_team": "Potomac Falls",
            "decision_type_code": "Fall",
            "fall_time": "1:23",
        }
    ),
    
    TestCase(
        name="Nested parentheses in loser team name",
        input_text="Prelim - Colton Allison (Louisa County) 1-2 won by fall over Quinn Cauthorne (Patrick Henry (Ashland)) 0-2 (Fall 3:18)",
        expected={
            "round_detail": "Prelim",
            "winner_name": "Colton Allison",
            "winner_team": "Louisa County",
            "decision_type": "fall",
            "loser_name": "Quinn Cauthorne",
            "loser_team": "Patrick Henry (Ashland)",
            "decision_type_code": "Fall",
            "fall_time": "3:18",
        }
    ),
    
    TestCase(
        name="Remove '-Forfeit' suffix from loser name",
        input_text="Cons. Round 3 - Chris Pyle (Roanoke Catholic) 9-7 won by forfeit over Keyanta Robinson-Forfeit (Amherst) 5-8 (For.)",
        expected={
            "round_detail": "Cons. Round 3",
            "winner_name": "Chris Pyle",
            "winner_team": "Roanoke Catholic",
            "decision_type": "forfeit",
            "loser_name": "Keyanta Robinson",
            "loser_team": "Amherst",
            "decision_type_code": "For.",
        }
    ),
    
    TestCase(
        name="Team name normalization: E9 -> E9; Scanlan Wrestling Academy -> Scanlan",
        input_text="Round 1 - John Doe (E9) 5-0 won by decision over Jane Smith (Scanlan Wrestling Academy) 3-2 (Dec 7-2)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "John Doe",
            "winner_team": "E9",
            "decision_type": "decision",
            "loser_name": "Jane Smith",
            "loser_team": "Scanlan",
            "decision_type_code": "Dec",
            "winner_points": 7,
            "loser_points": 2,
        }
    ),
    
    TestCase(
        name="Remove 'HS' suffix from team names",
        input_text="Round 1 - John Smith (Kellam HS) 5-0 won by decision over Jane Doe (Gloucester HS) 3-2 (Dec 7-5)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "John Smith",
            "winner_team": "Kellam",
            "decision_type": "decision",
            "loser_name": "Jane Doe",
            "loser_team": "Gloucester",
            "decision_type_code": "Dec",
            "winner_points": 7,
            "loser_points": 5,
        }
    ),
    
    TestCase(
        name="Remove 'Sr HS' suffix from team names",
        input_text="Semifinal - Alice Johnson (Akron Sr HS) 10-2 won by major decision over Bob Wilson (Canton Sr HS) 8-5 (MD 12-3)",
        expected={
            "round_detail": "Semifinal",
            "winner_name": "Alice Johnson",
            "winner_team": "Akron",
            "decision_type": "major decision",
            "loser_name": "Bob Wilson",
            "loser_team": "Canton",
            "decision_type_code": "MD",
            "winner_points": 12,
            "loser_points": 3,
        }
    ),
    
    TestCase(
        name="Remove dash-number suffix from team names (e.g., 'Averill Park-2' -> 'Averill Park')",
        input_text="Round 1 - Mike Thompson (Averill Park-2) 8-1 won by decision over Sarah Lee (Bethlehem-3) 5-4 (Dec 9-4)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Mike Thompson",
            "winner_team": "Averill Park",
            "decision_type": "decision",
            "loser_name": "Sarah Lee",
            "loser_team": "Bethlehem",
            "decision_type_code": "Dec",
            "winner_points": 9,
            "loser_points": 4,
        }
    ),
    
    TestCase(
        name="Transform APW/PUL to Altmar-Parish-Williamstown (Pulaski)",
        input_text="Round 1 - John Doe (APW/PUL) 5-0 won by decision over Jane Smith (Team B) 3-2 (Dec 7-3)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "John Doe",
            "winner_team": "Altmar-Parish-Williamstown (Pulaski)",
            "decision_type": "decision",
            "loser_name": "Jane Smith",
            "loser_team": "Team B",
            "decision_type_code": "Dec",
            "winner_points": 7,
            "loser_points": 3,
        }
    ),
    
    TestCase(
        name="Transform Altmar-Parish-Williamstown to Altmar-Parish-Williamstown (Pulaski)",
        input_text="Semifinal - Alice Brown (Altmar-Parish-Williamstown) 9-2 won by fall over Bob Green (Team C) 5-3 (Fall 3:45)",
        expected={
            "round_detail": "Semifinal",
            "winner_name": "Alice Brown",
            "winner_team": "Altmar-Parish-Williamstown (Pulaski)",
            "decision_type": "fall",
            "loser_name": "Bob Green",
            "loser_team": "Team C",
            "decision_type_code": "Fall",
            "fall_time": "3:45",
        }
    ),
    
    TestCase(
        name="Remove 'Jr HS' suffix from team names",
        input_text="Round 1 - Tom Wilson (Lincoln Jr HS) 7-2 won by decision over Sam Davis (Jefferson Jr HS) 4-3 (Dec 8-4)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Tom Wilson",
            "winner_team": "Lincoln",
            "decision_type": "decision",
            "loser_name": "Sam Davis",
            "loser_team": "Jefferson",
            "decision_type_code": "Dec",
            "winner_points": 8,
            "loser_points": 4,
        }
    ),
    
    TestCase(
        name="Remove 'Jr' suffix from team names",
        input_text="Quarterfinal - Emily Brown (Madison Jr) 9-1 won by major decision over Chris Lee (Washington Jr) 6-4 (MD 11-2)",
        expected={
            "round_detail": "Quarterfinal",
            "winner_name": "Emily Brown",
            "winner_team": "Madison",
            "decision_type": "major decision",
            "loser_name": "Chris Lee",
            "loser_team": "Washington",
            "decision_type_code": "MD",
            "winner_points": 11,
            "loser_points": 2,
        }
    ),
    
    TestCase(
        name="Remove dash-letter suffix from team names (e.g., 'Team-A' -> 'Team')",
        input_text="Round 1 - Alex Martinez (Central-A) 6-2 won by decision over Jordan Lee (Westside- C) 4-3 (Dec 8-5)",
        expected={
            "round_detail": "Round 1",
            "winner_name": "Alex Martinez",
            "winner_team": "Central",
            "decision_type": "decision",
            "loser_name": "Jordan Lee",
            "loser_team": "Westside",
            "decision_type_code": "Dec",
            "winner_points": 8,
            "loser_points": 5,
        }
    ),
]


def compare_results(actual: Dict[str, Any], expected: Dict[str, Any]) -> tuple[bool, List[str]]:
    """Compare actual and expected results, return (success, differences)."""
    differences = []
    
    # Check all expected fields
    for key, expected_value in expected.items():
        actual_value = actual.get(key)
        
        # Normalize None vs missing
        if expected_value is None and actual_value is None:
            continue
        
        # For string comparisons, ignore case and extra whitespace
        if isinstance(expected_value, str) and isinstance(actual_value, str):
            if expected_value.strip().lower() != actual_value.strip().lower():
                differences.append(f"  {key}: expected '{expected_value}', got '{actual_value}'")
        elif expected_value != actual_value:
            differences.append(f"  {key}: expected {expected_value!r}, got {actual_value!r}")
    
    return len(differences) == 0, differences


def run_tests():
    """Run all test cases and report results."""
    passed = 0
    failed = 0
    
    print(f"Running {len(TEST_CASES)} test cases...\n")
    print("=" * 80)
    
    for i, test in enumerate(TEST_CASES, 1):
        print(f"\n[{i}/{len(TEST_CASES)}] {test.name}")
        print(f"Input: {test.input_text[:80]}{'...' if len(test.input_text) > 80 else ''}")
        
        # Parse the match text
        actual = parse_match_text(test.input_text)
        
        # Compare results
        success, differences = compare_results(actual, test.expected)
        
        if success:
            print("✓ PASSED")
            passed += 1
        else:
            print("✗ FAILED")
            print("Differences:")
            for diff in differences:
                print(diff)
            print("\nFull actual result:")
            print(json.dumps(actual, indent=2))
            failed += 1
    
    # Summary
    print("\n" + "=" * 80)
    print(f"\nResults: {passed} passed, {failed} failed out of {len(TEST_CASES)} tests")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    exit(exit_code)
