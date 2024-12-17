# main.py
import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from src.data_collector import TennisDataCollector

async def main():
    database_url = 'postgresql://juanmarino@localhost:5432/college_tennis_db'
    collector = TennisDataCollector(database_url)

    try:
        data = await collector.fetch_all_matches()
        matches = data['data']['dualMatchesPaginated']['items']
        print(f"\nProcessing {len(matches)} Division I matches")

        for i, match in enumerate(matches, 1):
            try:
                print(f"\nProcessing match {i} of {len(matches)}")
                collector.store_single_match(match)
            except Exception as e:
                print(f"Error processing match {i}: {e}")
                continue

        print(f"\nCompleted storing {len(matches)} matches!")

    except Exception as e:
        print(f"Error in main process: {e}")
    
if __name__ == "__main__":
    asyncio.run(main())