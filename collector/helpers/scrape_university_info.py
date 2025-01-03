import requests
import re
import json
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from models.models import Base, Team, SchoolInfo

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def clean_js_object(js_obj_str: str) -> str:
    """Clean JavaScript object string to make it valid JSON"""
    js_obj_str = js_obj_str.strip(';')
    js_obj_str = js_obj_str.replace('""https":', '"https:')
    js_obj_str = re.sub(r'(\w+):', r'"\1":', js_obj_str)
    js_obj_str = re.sub(r'undefined', 'null', js_obj_str)
    js_obj_str = js_obj_str.replace("'#", '"#').replace("'", '"')
    return js_obj_str

def extract_configs(html_content: str) -> tuple[dict, dict]:
    """Extract and parse both configs from HTML content"""
    try:
        env_config_match = re.search(r"var envConfig = (\{.*?\});", html_content, re.S)
        team_config_match = re.search(r"var teamConfig = (\{.*?\});", html_content, re.S)
        
        env_config = {}
        team_config = {}
        
        if env_config_match:
            env_config_str = clean_js_object(env_config_match.group(1))
            try:
                env_config = json.loads(env_config_str)
            except json.JSONDecodeError as e:
                print(f"Error parsing envConfig: {e}")
                print(f"Problematic JSON: {env_config_str}")
        
        if team_config_match:
            team_config_str = clean_js_object(team_config_match.group(1))
            try:
                team_config = json.loads(team_config_str)
            except json.JSONDecodeError as e:
                print(f"Error parsing teamConfig: {e}")
                print(f"Problematic JSON: {team_config_str}")
        
        return env_config, team_config
        
    except Exception as e:
        print(f"Error extracting configs: {e}")
        return {}, {}

def extract_ids_from_raw_text(html_content: str) -> dict:
    """Extract IDs using regex directly from the raw text"""
    ids = {
        "school_id": None,
        "team_id": None
    }
    
    school_id_match = re.search(r'"schoolId":\s*"([^"]+)"', html_content)
    if school_id_match:
        ids["school_id"] = school_id_match.group(1)
    
    team_id_match = re.search(r'"teamId":\s*"([^"]+)"', html_content)
    if team_id_match:
        ids["team_id"] = team_id_match.group(1)
    
    return ids

def process_university_name(team_name: str) -> str:
    """Process team name into URL-friendly university name"""
    name = re.sub(r'\s*\([MW]\)\s*$', '', team_name)
    name = name.replace('.', '')
    name = re.sub(r'[^a-zA-Z\s]', '', name)
    name = ''.join(word.capitalize() for word in name.split())
    return name

def fetch_university_info(university_name: str) -> dict:
    """Fetch and parse university information"""
    processed_name = process_university_name(university_name)
    url = f"https://colleges.wearecollegetennis.com/{processed_name}M/Team"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code != 200:
            url = f"https://colleges.wearecollegetennis.com/{processed_name}W/Team"
            response = requests.get(url, headers=headers, verify=False)
        
        if response.status_code == 200:
            ids = extract_ids_from_raw_text(response.text)
            env_config, team_config = extract_configs(response.text)
            
            result = {
                "original_name": university_name,
                "processed_name": processed_name,
                "url": url,
                "ids": ids
            }
            
            return result
            
        else:
            print(f"Failed to fetch data: Status {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}

def fetch_school_data(school_id: str) -> dict:
    """Fetch school data using the school GraphQL query"""
    url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
    query = """
    query school { 
        school(id: "%s") { 
            id 
            name 
            conference 
            itaRegion 
            rankingAwardRegion 
            ustaSection 
            manId 
            womanId 
            division 
            mailingAddress 
            city 
            state 
            zipCode 
            teamType 
        }
    }
    """ % school_id

    try:
        response = requests.post(
            url,
            json={'query': query},
            headers={'Content-Type': 'application/json'},
            verify=False
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching school data: Status {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"Error fetching school data: {e}")
        return {}

def process_teams_from_db():
    """Process team names from database and store school IDs"""
    # Track successes and failures
    successful_schools = []
    failed_schools = []
    skipped_schools = []
    processed_school_ids = set()

    database_url = 'postgresql://juanmarino@localhost:5432/college_tennis_db'
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        teams = session.query(Team.name).distinct().all()
        total_teams = len(teams)
        print(f"Found {total_teams} teams to process")

        success_count = 0
        failure_count = 0
        already_processed = 0

        for i, (team_name,) in enumerate(teams, 1):
            try:
                print(f"\nProcessing {i}/{total_teams}: {team_name}")
                
                # Check if already processed
                base_name = re.sub(r'\s*\([MW]\)\s*$', '', team_name)
                existing_school = session.query(SchoolInfo)\
                    .filter(SchoolInfo.name.ilike(f"%{base_name}%"))\
                    .first()

                if existing_school:
                    print(f"Already processed: {base_name}")
                    skipped_schools.append({
                        'name': team_name,
                        'base_name': base_name,
                        'existing_id': existing_school.id
                    })
                    already_processed += 1
                    continue

                # Get school ID from website
                result = fetch_university_info(team_name)
                if result and result.get('ids', {}).get('school_id'):
                    school_id = result['ids']['school_id']
                    
                    if school_id in processed_school_ids:
                        skipped_schools.append({
                            'name': team_name,
                            'reason': 'School ID already processed',
                            'school_id': school_id
                        })
                        already_processed += 1
                        continue

                    processed_school_ids.add(school_id)
                    school_data = fetch_school_data(school_id)
                    
                    if school_data and 'data' in school_data and 'school' in school_data['data']:
                        school_info = school_data['data']['school']
                        
                        new_school = SchoolInfo(
                            id=school_info['id']
                        )
                        
                        session.add(new_school)
                        session.commit()
                        success_count += 1
                        
                        successful_schools.append({
                            'name': team_name,
                            'processed_name': result['processed_name'],
                            'school_id': school_id,
                            'school_name': school_info['name']
                        })
                        
                        print(f"Successfully stored: {school_info['name']}")
                    else:
                        failed_schools.append({
                            'name': team_name,
                            'reason': 'Failed to fetch school data',
                            'processed_name': result['processed_name']
                        })
                        failure_count += 1
                else:
                    failed_schools.append({
                        'name': team_name,
                        'reason': 'Failed to get school ID',
                        'processed_name': result['processed_name'] if result else None
                    })
                    failure_count += 1

            except Exception as e:
                failed_schools.append({
                    'name': team_name,
                    'reason': str(e),
                    'processed_name': result['processed_name'] if 'result' in locals() else None
                })
                failure_count += 1
                session.rollback()
                continue

    except Exception as e:
        print(f"Unexpected error: {e}")
        session.rollback()
    finally:
        session.close()
        
        # Print detailed report
        print("\n=== PROCESSING REPORT ===")
        
        print(f"\nFailed Schools ({len(failed_schools)}):")
        for school in failed_schools:
            print(f"✗ {school['name']}")
            print(f"  Reason: {school['reason']}")
            if school['processed_name']:
                print(f"  Processed as: {school['processed_name']}")
        
        # Save report to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f'school_processing_report_{timestamp}.txt', 'w') as f:
            f.write("=== SCHOOL PROCESSING REPORT ===\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("SUCCESSFUL SCHOOLS:\n")
            for school in successful_schools:
                f.write(f"{school['name']} -> {school['school_name']}\n")
                f.write(f"ID: {school['school_id']}\n")
                f.write(f"Processed as: {school['processed_name']}\n\n")
            
            f.write("\nFAILED SCHOOLS:\n")
            for school in failed_schools:
                f.write(f"{school['name']}\n")
                f.write(f"Reason: {school['reason']}\n")
                if school['processed_name']:
                    f.write(f"Processed as: {school['processed_name']}\n")
                f.write("\n")

            f.write(f"\nSUMMARY:\n")
            f.write(f"Successful: {success_count}\n")
            f.write(f"Failed: {failure_count}\n")
            f.write(f"Already processed: {already_processed}\n")
            f.write(f"Total teams: {total_teams}\n")

if __name__ == "__main__":
    process_teams_from_db()