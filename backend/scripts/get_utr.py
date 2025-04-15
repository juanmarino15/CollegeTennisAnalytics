import pickle
import os
import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class UTRSeleniumSession:
    def __init__(self, cookies_file='utr_cookies.pkl'):
        self.cookies_file = cookies_file
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
            "Origin": "https://app.utrsports.net",
            "Referer": "https://app.utrsports.net/",
            "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "x-client-name": "buildId - 120416"
        })
    
    def get_cookies_with_selenium(self, email, password):
        """Use Selenium to log in and extract cookies"""
        options = Options()
        # options.add_argument("--headless")  # Run in background
        options.add_argument("--window-size=1920,1080")
        
        driver = webdriver.Chrome(options=options)
        
        try:
            # Go to login page
            print("Opening UTR login page...")
            driver.get("https://app.utrsports.net/login")
            
            # Wait for page to load completely
            time.sleep(5)  # Give the page time to fully render
            
            print("Page loaded. Looking for login form...")
            
            # Find email field using the specific ID from the HTML
            try:
                print("Looking for email field with ID 'emailInput'")
                email_field = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "emailInput"))
                )
                print("Found email field!")
            except Exception as e:
                print(f"Error finding email field: {e}")
                driver.save_screenshot("login_page.png")
                print("Screenshot saved as login_page.png")
                raise
            
            # Find password field using the specific ID
            try:
                print("Looking for password field with ID 'passwordInput'")
                password_field = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "passwordInput"))
                )
                print("Found password field!")
            except Exception as e:
                print(f"Error finding password field: {e}")
                driver.save_screenshot("login_form.png")
                print("Screenshot saved as login_form.png")
                raise
            
            # Clear fields and enter credentials
            print("Entering credentials...")
            email_field.clear()
            email_field.send_keys(email)
            
            password_field.clear()
            password_field.send_keys(password)
            
            # Find login button - using the exact button class from the HTML
            try:
                print("Looking for login button...")
                login_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-primary') and contains(text(), 'SIGN IN')]"))
                )
                print("Found login button!")
            except Exception as e:
                print(f"Error finding login button: {e}")
                driver.save_screenshot("login_button.png")
                print("Screenshot saved as login_button.png")
                raise
            
            # Click login button
            print("Clicking login button...")
            login_button.click()
            
            # Wait for login to complete
            try:
                print("Waiting for login to complete...")
                # Wait for JWT cookie to appear
                WebDriverWait(driver, 20).until(
                    lambda driver: "jwt" in [cookie['name'] for cookie in driver.get_cookies()]
                )
                print("Login successful!")
            except TimeoutException:
                print("Timeout waiting for login to complete")
                driver.save_screenshot("after_login.png")
                print("Screenshot saved as after_login.png")
                raise
            
            # Wait a moment for all cookies to be set
            time.sleep(3)
            
            # Now go to the players page to make sure all cookies are set
            print("Navigating to players page to ensure all cookies are set...")
            driver.get("https://app.utrsports.net/players")
            time.sleep(5)
            
            # Extract cookies
            print("Extracting cookies...")
            cookies = driver.get_cookies()
            
            # Convert to dictionary format
            cookie_dict = {}
            for cookie in cookies:
                cookie_dict[cookie['name']] = cookie['value']
                print(f"Found cookie: {cookie['name']}")
            
            # Look specifically for JWT cookie
            if 'jwt' in cookie_dict:
                print("Found JWT cookie")
            else:
                print("WARNING: JWT cookie not found!")
                
            # Save cookies to file
            with open(self.cookies_file, 'wb') as f:
                pickle.dump(cookie_dict, f)
            
            print("Cookies saved to file!")
            
            # Load cookies into requests session
            self.session.cookies.update(cookie_dict)
            
            return cookie_dict
            
        except Exception as e:
            print(f"Error during Selenium automation: {e}")
            return None
        finally:
            print("Closing browser...")
            time.sleep(2)  # Give a moment to see final state
            driver.quit()
    
    def search_players(self, top=40, skip=0, query=None, min_utr=None, max_utr=None):
        """Search for players using the UTR API"""
        url = "https://api.utrsports.net/v2/search/players"
        
        # Base parameters
        params = {
            "top": top,
            "skip": skip,
            "utrTeamType": "singles",
            "showTennisContent": "true",
            "showPickleballContent": "true",
            "searchOrigin": "searchPage"
        }
        
        # Add optional parameters if provided
        if query:
            params["query"] = query
        
        if min_utr:
            params["minUtr"] = min_utr
            
        if max_utr:
            params["maxUtr"] = max_utr
        
        try:
            # Print the cookies we're using for debugging
            print("Using cookies for request:")
            for name, value in self.session.cookies.items():
                if name == 'jwt':
                    print(f"  JWT: {value[:20]}...")  # Only show beginning of JWT for security
                else:
                    print(f"  {name}: {value[:20]}..." if len(str(value)) > 20 else f"  {name}: {value}")
            
            # Add explicit headers for the request
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
                "Origin": "https://app.utrsports.net",
                "Referer": "https://app.utrsports.net/",
                "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "x-client-name": "buildId - 120416"
            }
            
            # Make request with all headers and cookies
            response = self.session.get(url, params=params, headers=headers)
            
            print(f"Request URL: {response.url}")
            print(f"Status Code: {response.status_code}")
            
            # Check if the response is valid
            if response.status_code != 200:
                print(f"API request failed with status code: {response.status_code}")
                print(f"Response content: {response.text[:500]}")  # Show first 500 chars of response
                return None
            
            # Parse JSON response
            data = response.json()
            
            # Check if 'hits' key exists in response (based on the provided JSON structure)
            players = []
            if 'hits' in data:
                # Extract player data from the hits array
                for hit in data['hits']:
                    if 'source' in hit:
                        players.append(hit['source'])
                
                print(f"Search successful, found {len(players)} players")
                return {
                    'players': players,
                    'totalCount': len(data['hits'])
                }
            else:
                print("Warning: 'hits' key not found in response")
                return {
                    'players': [],
                    'totalCount': 0
                }
            
        except Exception as e:
            print(f"Error searching players: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text[:500]}")
            return None
    
    def get_all_players(self, total=100, query=None, min_utr=None, max_utr=None):
        """Get multiple pages of players up to the specified total"""
        all_players = []
        skip = 0
        page_size = 40  # UTR's default page size
        
        while len(all_players) < total:
            # Calculate how many players to request in this batch
            batch_size = min(page_size, total - len(all_players))
            if batch_size <= 0:
                break
                
            print(f"Fetching players {skip+1}-{skip+batch_size}...")
            
            # Make the API request
            data = self.search_players(
                top=batch_size, 
                skip=skip, 
                query=query,
                min_utr=min_utr,
                max_utr=max_utr
            )
            
            # Check if we got valid data
            if not data or 'players' not in data:
                print("No data returned or error occurred")
                break
                
            # Extract players from response
            players = data['players']
            
            # Check if we reached the end of results
            if not players:
                print("No more players available")
                break
                
            # Add players to our collection
            all_players.extend(players)
            print(f"Retrieved {len(players)} players (total: {len(all_players)})")
            
            # If we got fewer players than requested, we've reached the end
            if len(players) < batch_size:
                print("Reached end of results")
                break
                
            # Update skip for next batch
            skip += len(players)
            
            # Add a small delay to avoid rate limiting
            time.sleep(1)
        
        # Save raw results to file
        result = {
            "players": all_players,
            "totalCount": len(all_players)
        }
        
        with open("utr_all_players.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4)
            print(f"Saved {len(all_players)} players to utr_all_players.json")
            
        return all_players
    
    def extract_player_details(self, players):
        """Extract relevant details from player data for easier analysis"""
        simplified_data = []
        
        for player in players:
            # Extract basic info
            player_info = {
                "id": player.get("id"),
                "firstName": player.get("firstName"),
                "lastName": player.get("lastName"),
                "displayName": player.get("displayName"),
                "gender": player.get("gender"),
                "nationality": player.get("nationality"),
                "isPro": player.get("isPro"),
                "ageRange": player.get("ageRange")
            }
            
            # Extract location if available
            if "location" in player and player["location"]:
                player_info["location"] = player["location"].get("display")
            
            # Extract UTR ratings
            player_info["singlesUtr"] = player.get("singlesUtrDisplay")
            player_info["doublesUtr"] = player.get("doublesUtrDisplay")
            player_info["singlesUtrNumeric"] = player.get("singlesUtr")
            player_info["doublesUtrNumeric"] = player.get("doublesUtr")
            
            # Extract ratings status
            player_info["singlesRatingStatus"] = player.get("ratingStatusSingles")
            player_info["doublesRatingStatus"] = player.get("ratingStatusDoubles")
            
            # Extract rankings if available
            if "rankings" in player and player["rankings"]:
                player_info["rankings"] = []
                for ranking in player["rankings"]:
                    player_info["rankings"].append({
                        "rankListId": ranking.get("rankListId"),
                        "rank": ranking.get("rank")
                    })
            
            # Extract third-party rankings if available
            if "thirdPartyRankings" in player and player["thirdPartyRankings"]:
                player_info["thirdPartyRankings"] = []
                for ranking in player["thirdPartyRankings"]:
                    player_info["thirdPartyRankings"].append({
                        "source": ranking.get("source"),
                        "type": ranking.get("type"),
                        "rank": ranking.get("rank")
                    })
            
            # Add to collection
            simplified_data.append(player_info)
        
        # Save simplified data to file
        with open("utr_players_simplified.json", "w", encoding="utf-8") as f:
            json.dump(simplified_data, f, indent=4)
            print(f"Saved {len(simplified_data)} simplified player records to utr_players_simplified.json")
        
        return simplified_data
    
    def save_csv(self, players, filename="utr_players.csv"):
        """Save player data to CSV file for easy analysis"""
        import csv
        
        # Define fields to include in CSV
        fields = [
            "id", "firstName", "lastName", "displayName", "gender", 
            "singlesUtr", "doublesUtr", "nationality", "isPro",
            "location", "ageRange"
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction='ignore')
                writer.writeheader()
                
                for player in players:
                    row = {}
                    for field in fields:
                        if field == "location" and isinstance(player.get("location"), dict):
                            row[field] = player["location"].get("display", "")
                        else:
                            row[field] = player.get(field, "")
                    writer.writerow(row)
                
            print(f"Successfully saved {len(players)} players to {filename}")
            return True
        except Exception as e:
            print(f"Error saving CSV: {e}")
            return False

# Usage
if __name__ == "__main__":
    utr = UTRSeleniumSession()
    
    # Try to load existing cookies first
    cookies_exist = False
    if os.path.exists(utr.cookies_file):
        try:
            with open(utr.cookies_file, 'rb') as f:
                cookies = pickle.load(f)
                utr.session.cookies.update(cookies)
                
            # Test if cookies work with player search API
            test_response = utr.session.get(
                "https://api.utrsports.net/v2/search/players?top=1",
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://app.utrsports.net",
                    "Referer": "https://app.utrsports.net/"
                }
            )
            if test_response.status_code == 200:
                print("Using existing cookies")
                cookies_exist = True
            else:
                print(f"Existing cookies are invalid: {test_response.status_code}")
        except Exception as e:
            print(f"Error loading cookies: {e}")
    
    # If no cookies or invalid, get new ones
    if not cookies_exist:
        # Replace with your credentials
        cookies = utr.get_cookies_with_selenium(
            email="juasmarino@gmail.com", 
            password="Promocion2011"
        )
    
    if utr.session.cookies:
        # Example: Search for top 100 players
        players = utr.get_all_players(
            total=100,
            # Optional parameters:
            # query="Nadal",  # Search for players with "Nadal" in their name
            # min_utr=10,     # Minimum UTR rating of 10
            # max_utr=16      # Maximum UTR rating of 16
        )
        
        print(f"Total players retrieved: {len(players)}")
        
        if players:
            # Extract and save simplified player data
            simplified_data = utr.extract_player_details(players)
            print(f"Successfully extracted details for {len(simplified_data)} players")
            
            # Save as CSV for easy analysis in Excel/Google Sheets
            utr.save_csv(players)
    else:
        print("Failed to get cookies, cannot proceed with player search")