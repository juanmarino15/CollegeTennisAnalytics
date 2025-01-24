import httpx
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict
import asyncio

async def find_match():
   api_url = 'https://prd-itat-kube-tournamentdesk-api.clubspark.pro/'
   skip = 0
   limit = 100
   max_retries = 3
   
   query = """
   query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
       dualMatchesPaginated(skip: $skip, limit: $limit, filter: $filter, sort: $sort) {
           totalItems
           items {
               id
               startDateTime {
                   timezoneName
                   noScheduledTime
                   dateTimeString
                   __typename
               }
               homeTeam {
                   name
                   abbreviation
                   id
                   division
                   conference
                   region
                   score
                   didWin
                   sideNumber
                   __typename
               }
               teams {
                   name
                   abbreviation
                   id
                   division
                   conference
                   region
                   score
                   didWin
                   sideNumber
                   __typename
               }
               isConferenceMatch
               gender
               webLinks {
                   name
                   url
                   __typename
               }
               __typename
           }
       }
   }
   """

   while True:
       for retry in range(max_retries):
           try:
               variables = {
                   "skip": skip,
                   "limit": limit,
                   "sort": {
                       "field": "START_DATE",
                       "direction": "DESCENDING"
                   },
                   "filter": {
                       "seasonStarting": "2024", 
                       "isCompleted": False,
                       "divisions": ["DIVISION_1"]
                   }
               }

               async with httpx.AsyncClient(verify=False) as client:
                   print(f"\nFetching matches {skip} to {skip+limit}")
                   response = await client.post(
                       api_url,
                       json={
                           "operationName": "dualMatchesPaginated",
                           "query": query,
                           "variables": variables
                       },
                       timeout=30.0
                   )

                   if response.status_code == 200:
                       data = response.json()
                       matches = data.get('data', {}).get('dualMatchesPaginated', {}).get('items', [])
                       total = data.get('data', {}).get('dualMatchesPaginated', {}).get('totalItems', 0)
                       
                       print(f"Found {len(matches)} matches (Total: {total})")
                       
                       for match in matches:
                           match_id = match.get('id')
                           date_str = match.get('startDateTime', {}).get('dateTimeString')
                           print(f"ID: {match_id}, Date: {date_str}")
                           
                           if match_id == 'B9A3CED8-F80E-4531-910A-FA236EA86C80':
                               print("\nFOUND TARGET MATCH:")
                               print(json.dumps(match, indent=2))
                               return

                       if not matches:
                           print("\nNo more matches to process")
                           return
                           
                       skip += limit
                       await asyncio.sleep(2)  # Rate limiting
                       break  # Successful request, break retry loop
                       
                   elif response.status_code == 502 and retry < max_retries - 1:
                       print(f"Got 502, attempt {retry + 1}/{max_retries}, retrying...")
                       await asyncio.sleep(5)  # Wait longer between retries
                   else:
                       print(f"Error: {response.status_code}")
                       print(response.text)
                       return

           except Exception as e:
               print(f"Error: {str(e)}")
               if retry < max_retries - 1:
                   await asyncio.sleep(5)
               else:
                   return

if __name__ == "__main__":
   asyncio.run(find_match())