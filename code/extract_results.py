from bs4 import BeautifulSoup
from datetime import datetime
import os
import csv
import re

## FUNCTION DEFS ##
# Takes a directory path and a list to store all matches
def parse_html_files_in_directory(directory_path, all_matches):
    # List all files in the directory
    print("Parsing HTML files in directory: ", directory_path)
    for filename in os.listdir(directory_path):
        # Construct full file path
        file_path = os.path.join(directory_path, filename)
        # Check if it is a file (not a directory)
        if os.path.isfile(file_path) and file_path.endswith('.html'):
            print(f"Parsing file: {file_path}")
            parse_html_nodes_from_file(file_path, all_matches, directory_path)

def parse_html_nodes_from_file(file_path, all_matches, directory):
    # Read the HTML content from the file
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        html_content = file.read()
    
    # Create a BeautifulSoup object and specify the parser
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all nodes (for example, all div elements)
    top = soup.find('font')
    sections = top.find_all('section', recursive=False)

    # Use number of sections found to determine how to parse the file
    if len(sections) == 1:
        parse_advanced_tournament(sections[0], all_matches, file_path, directory)
    else: 
        parse_basic_tournament(sections, all_matches, file_path, directory)

def parse_basic_tournament(sections, all_matches, filename, directory):
    print ("--> Parsing " + str(len(sections)) + " Matches")
    filename_parse = parse_filenames(filename, directory)
    for section in sections:
        match = {}
        match.update(filename_parse)
        subsections = section.find_all('section', recursive=False)
        # Don't parse this entry if there is no results text or if it is a BYE
        if subsections[1].find('li') is None or "BYE" in subsections[1].find('li').text.upper() or "DFF" in subsections[1].find('li').text.upper():
            continue
        match["class"] = section.find('h1').text
        match["round"] = subsections[1].find('h2').text
        match["bracket"] = subsections[0].find('h2').text
        results = parse_results_text(subsections[1].find('li').text)
        match.update(results)
        match["notes"] = subsections[0].find('li').text
        all_matches.append(match)
    
def parse_advanced_tournament(section, all_matches, filename, directory):
    filename_parse = parse_filenames(filename, directory)
    round = section.find('h1').text
    classes = section.find_all('h2')
    print ("--> Parsing Matches for " + str(len(classes)) + " Weight Classes")
    for i in range(len(classes)):
        class_text = classes[i].text
        class_matches_list = classes[i].find_next_sibling('ul')
        class_matches = class_matches_list.find_all('li')
        print ("----> Parsing " + str(len(class_matches)) + " Matches From " + class_text + " Class")
        for class_match in class_matches:
            #Don't parse this entry if it is a BYE or DFF
            if "BYE" in class_match.text.upper() or "DFF" in class_match.text.upper():
                continue
            results = parse_results_text(class_match.text)
            match = {}
            match.update(filename_parse)
            match["class"] = class_text
            match["round"] = round
            match["bracket"] = "" # Will be overwritten by text parse results, generally
            match["notes"] = ""
            match.update(results)
            all_matches.append(match)

def parse_filenames(text, directory):
    match = {}
    # Parse on "/", "\", ".", and the term " - "
    base_text = text[text.find(directory) + len(directory) + 1:]
    result = re.split("\.| - ", base_text)
    # Strip whitespace and remove empty strings
    stripped = [s.strip() for s in result]
    filtered = list(filter(lambda x: x != "", stripped))
    remove_text = [
        " Bantam-Mids",
        " Ban-Mids",
        " Int-Srs",
        " Juniors",
        " R1-16man",
        " R1",
        " R2A",
        " R2",
        " R3",
        " R4",
        " R5",
        " 1st wrestleback",
        " 2nd wrestleback",
        " 3rd wrestleback",
        " 4th wrestleback",
        " Placement Matches",
        " placement matches"
    ]
    # Remove configured text from tournament name
    for term in remove_text:
        filtered[1] = filtered[1].replace(term, "")

    match["tournament"] = filtered[1]
    match["date"] = datetime.strptime(filtered[0], '%Y_%m_%d').date()
    return match

def parse_results_text(text):
    match = {}
    # Special case, remove (kj) from text, which throws off parentheses parsing
    # Parse on "(", ")", and the word "over"
    result = re.split(" over |[()]", text.replace(" (kj) ", ""))
    # Strip whitespace and remove empty strings
    stripped = [s.strip() for s in result]
    filtered = list(filter(lambda x: x != "", stripped))
    if filtered[0].find(' - ') != -1:
        match["bracket"] = filtered[0].split(' - ')[0]
        match["winner"] = filtered[0].split(' - ')[1]
    else:
        match["winner"] = filtered[0]
    match["winner_team"] = transform_team(filtered[1])
    match["over"] = filtered[3]
    match["over_team"] = transform_team(filtered[4])
    match["decision"] = filtered[2]
    decision_dets = filtered[5].split(" ")
    match["decision_type"] = decision_dets[0]
    if len(decision_dets) < 2:
        match["fall_time"] = None
        match["winner_score"] = None
        match["over_score"] = None
    elif decision_dets[1].find(":") != -1:
        match["fall_time"] = decision_dets[1]
        match["winner_score"] = None
        match["over_score"] = None
    else:
        match["fall_time"] = None
        match["winner_score"] = decision_dets[1].split("-")[0]
        match["over_score"] = decision_dets[1].split("-")[1]
    return match

# Function to input apply a list of string transformationg to an input string
def transform_team(input_string):
    transformed_string = input_string
    transformations = [
        ["Alexandria Junior Titans", "Alexandria"],
        ["Annandale Mat Rats", "Annendale"],
        ["Braddock Wrestling Club", "Braddock"],
        ["E9Wrestling", "E9 Wrestling"],
        ["Fauquier Wrestling", "Fauquier"],
        ["FortBelvoir", "Fort Belvoir"],
        ["Franconia Wrestling Club", "Franconia"],
        ["Gunston Wrestling Club", "Gunston"],
        ["HerndonHawks", "Herndon Hawks"],
        ["KingGeorge", "King George"],
        ["King George Wrestling Club", "King George"],
        ["Alexandria", "Alexandria Junior Titans"],
        ["McLean Lion Wrestling", "McLean"],
        ["McLean Lions Wrestling", "McLean"],
        ["Mount Vernon Youth Wrestling", "Mt Vernon"],
        ["MountVernon", "Mt Vernon"],
        ["PitBull", "Pit Bull"],
        ["Prince William County Wrestling Club", "Prince William"],
        ["Prince William Wrestling Club", "Prince William"],
        ["PrinceWilliam", "Prince William"],
        ["Scanlon Wrestling", "Scanlan"],
        ["Smyrna Wrestling", "Smyrna"],
        ["South County Athletic Association", "South County"],
        ["SouthCounty", "South County"],
        ["Vienna Youth Inc", "Vienna"],
        ["Vikings Wrestling Club", "Vikings"],
        ["Viking Wrestling Club", "Vikings"],
        ["WildBuffalos", "Wild Buffalos"]
    ]
    for transformation in transformations:
        transformed_string = transformed_string.replace(transformation[0], transformation[1])
    return transformed_string

# Function to convert mm:ss to seconds
def convert_time_to_seconds(time):
    if time is None:
        return None
    time_parts = time.split(":")
    return int(time_parts[0]) * 60 + int(time_parts[1])

### MAIN PROGRAM START ###
directory_path = 'Raw Results'
all_matches = []
parse_html_files_in_directory(directory_path, all_matches)

# Get a list of all wrestlers and teams
all_teams = []
all_wrestlers = []
all_tournaments = []
all_seasons = []
for match in all_matches:
    # Add a tournament display field, which includes the date of tournament
    match["tournament_display"] = match["tournament"] + " (" + match["date"].strftime("%m/%d/%Y") + ")"
    # Add a season field if month of year is 9 or later
    if match["date"].month >= 9:
        match["season"] = str(match["date"].year) + "-" + str(match["date"].year + 1)
    else:
        match["season"] = str(match["date"].year - 1) + "-" + str(match["date"].year)
    # Add a fall seconds computation
    match["fall_seconds"] = convert_time_to_seconds(match["fall_time"])

    # Add records to unique lists if they are not already present
    if match["season"] not in all_seasons:
        all_seasons.append(match["season"])
    if match["winner_team"] not in all_teams:
        all_teams.append(match["winner_team"])
    if match["over_team"] not in all_teams:
        all_teams.append(match["over_team"])
    if match["winner"] not in all_wrestlers:
        all_wrestlers.append(match["winner"])
    if match["over"] not in all_wrestlers:
        all_wrestlers.append(match["over"])
    if match["tournament_display"] not in all_tournaments:
        all_tournaments.append(match["tournament_display"])

# Function to group input array by selected field and output an object with the field as key
def group_by_field(input_array, field):
    output = {}
    for item in input_array:
        if item[field] not in output:
            output[item[field]] = []
        output[field].append(item)
    return output


# Turn all_matches into pandas dataframe
import pandas as pd
df = pd.DataFrame(all_matches)




all_seasons.sort()
all_teams.sort()
all_wrestlers.sort()
all_tournaments.sort()

print(all_teams, all_wrestlers, all_tournaments, all_seasons)



# Output results to a CSV file
output_file_path = 'parsed_matches.csv'
with open(output_file_path, 'w', newline='', encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=all_matches[0].keys())
    writer.writeheader()
    for match in all_matches:
        writer.writerow(match)