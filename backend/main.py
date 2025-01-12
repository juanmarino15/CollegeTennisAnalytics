# main.py
import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "collector"))

from collector.data_collector import TennisDataCollector

async def main():
    # database_url = 'postgresql://juanmarino@localhost:5432/college_tennis_db'
    database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

    collector = TennisDataCollector(database_url)

    try:
        # Fetch and store completed matches
        # completed_data = await collector.fetch_all_matches()
        # completed_matches = completed_data['data']['dualMatchesPaginated']['items']
        # print(f"\nProcessing {len(completed_matches)} completed Division I matches")

        # for i, match in enumerate(completed_matches, 1):
        #     try:
        #         print(f"\nProcessing completed match {i} of {len(completed_matches)}")
        #         collector.store_single_match(match)
        #     except Exception as e:
        #         print(f"Error processing completed match {i}: {e}")
        #         continue

        # # Fetch and store upcoming matches
        # upcoming_data = await collector.fetch_upcoming_matches()
        # upcoming_matches = upcoming_data['data']['dualMatchesPaginated']['items']
        # print(f"\nProcessing {len(upcoming_matches)} upcoming Division I matches")

        # for i, match in enumerate(upcoming_matches, 1):
        #     try:
        #         print(f"\nProcessing upcoming match {i} of {len(upcoming_matches)}")
        #         collector.store_single_match(match)
        #     except Exception as e:
        #         print(f"Error processing upcoming match {i}: {e}")
        #         continue

        # print(f"\nCompleted storing {len(upcoming_matches)} upcoming matches!")
    
        # Print initial counts
        # counts = collector.get_teams_with_logos_count()
        # print(f"Before fetching:")
        # print(f"Total teams: {counts['total_teams']}")
        # print(f"Teams with logos: {counts['with_logos']}")
        # print(f"Teams without logos: {counts['without_logos']}")
        
        # # # Fetch and store logos
        # await collector.fetch_and_store_team_logos()
        
        # # # Print final counts
        # counts = collector.get_teams_with_logos_count()
        # print(f"\nAfter fetching:")
        # print(f"Total teams: {counts['total_teams']}")
        # print(f"Teams with logos: {counts['with_logos']}")
        # print(f"Teams without logos: {counts['without_logos']}")

        # await collector.test_single_logo_fetch()
        #Test with a random team that has a logo
    #     team_id, team_name = collector.get_random_team_with_logo()
    #     if team_id:
    #         print(f"\nTesting with random team: {team_name}")
    #         collector.test_retrieve_logo(team_id, f"{team_name.replace(' ', '_')}_logo.png")

    # except Exception as e:
    #     print(f"Error in main process: {e}")

     # Update school details
        # print("Updating school details...")
        # collector.update_school_details()
        # print("School details update completed!")
     
        # # Store season information
        # print("Storing season information...")
        # collector.store_seasons()
        # print("Season information stored!")

        # Process all rosters for current season
        season_id = "0d09ee6d-c173-4d98-8207-7c944409faf0"  # 2024 season
        print("Processing rosters...")
        collector.process_all_rosters(season_id)
        print("Roster processing completed!")

        #Store all player matches
        # print("Starting to store all player matches...")
        # collector.store_all_player_matches()
        # print("Completed storing all player matches!")

        #store dual matches score box
        # print("Starting to store all match lineups...")
        # collector.store_all_match_lineups()
        # print("Completed storing all match lineups!")

    except Exception as e:
        print(f"Error in main process: {e}")

    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    asyncio.run(main())