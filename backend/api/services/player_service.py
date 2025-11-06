from sqlalchemy.orm import Session
from models import Player, PlayerSeason, PlayerRoster, PlayerWTN, PlayerMatch, PlayerMatchParticipant,Team,MatchLineup,SchoolInfo,Season,Match,PlayerSearchView
from typing import Optional, List
from sqlalchemy import func, or_, text
from datetime import datetime

class PlayerService:
    def __init__(self, db: Session):
        self.db = db
    
    def _player_to_dict(self, player):
        return {
            "person_id": player.person_id,
            "tennis_id": player.tennis_id,
            "first_name": player.first_name,
            "last_name": player.last_name,
            "avatar_url": player.avatar_url
        }
    
    def _wtn_to_dict(self, wtn):
        """Convert PlayerWTN model to dictionary with proper serialization"""
        return {
            "person_id": wtn.person_id,
            "tennis_id": wtn.tennis_id,
            "season_id": wtn.season_id,
            "wtn_type": wtn.wtn_type,
            "confidence": wtn.confidence,
            "tennis_number": float(wtn.tennis_number) if wtn.tennis_number is not None else None,  # Ensure proper float conversion
            "is_ranked": wtn.is_ranked
        }
    
    def _season_to_dict(self, season):
        return {
            "person_id": season.person_id,
            "tennis_id": season.tennis_id,
            "season_id": season.season_id,
            "class_year": season.class_year
        }

    def _match_to_dict(self, match):
        return {
            "id": match.id,
            "start_time": match.start_time,
            "end_time": match.end_time,
            "match_type": match.match_type,
            "match_format": match.match_format,
            "status": match.status,
            "score_string": match.score_string,
            "collection_position": match.collection_position  

        }

    def get_players(self, team_id: str = None):
        query = self.db.query(Player)
        if team_id:
            upper_team_id = team_id.upper()
            query = query.join(PlayerRoster).filter(
                func.upper(PlayerRoster.team_id) == upper_team_id
            )
        players = query.all()
        return [self._player_to_dict(player) for player in players]

    def get_player(self, player_id: str):
        if player_id:
            upper_player_id = player_id.upper()
            player = self.db.query(Player).filter(
                func.upper(Player.person_id) == upper_player_id
            ).first()
            return self._player_to_dict(player) if player else None
        return None


    def get_player_wtn(self, player_id: str, season: Optional[str] = None):
        """Get World Tennis Number (WTN) ratings for a player, optionally filtered by season"""
        if not player_id:
            return []
        
        upper_player_id = player_id.upper() if player_id else None
        
        # Query WTN data
        query = self.db.query(PlayerWTN).filter(
            func.upper(PlayerWTN.person_id) == upper_player_id
        )
        
        # Add season filter if provided
        if season:
            # Find the season_id that corresponds to the year
            season_obj = self.db.query(Season).filter(
                Season.name == season
            ).first()
            
            if season_obj:
                # If season found, filter by season_id
                query = query.filter(PlayerWTN.season_id == season_obj.id)
                print(f"Filtering WTN by season: {season} (ID: {season_obj.id})")
            else:
                print(f"Season not found: {season}")
        
        # Execute query and get results
        wtns = query.all()
        
        # Log for debugging
        print(f"Found {len(wtns)} WTN records for player {player_id}")
        for wtn in wtns:
            print(f"  Type: {wtn.wtn_type}, Number: {wtn.tennis_number}, Season ID: {wtn.season_id}")
        
        # Convert to dict and return
        return [self._wtn_to_dict(wtn) for wtn in wtns]

    def get_player_team(self, player_id: str, season: Optional[str] = None):
        """Get the team info for a player in a specific season"""
        if not player_id:
            return None
        
        upper_player_id = player_id.upper()
        
        print(f"🔍 Looking up team for player_id: {player_id}, season: {season}")
        
        # Get roster entry for player
        roster_query = self.db.query(PlayerRoster).filter(
            func.upper(PlayerRoster.person_id) == upper_player_id,
            PlayerRoster.active == True  

        )
        
        # Add season filter if provided
        if season:
            print(f"Filtering by season: {season}")
            
            # Try exact match first (e.g., "2024-2025")
            season_obj = self.db.query(Season).filter(
                Season.name == season
            ).first()
            
            # If no exact match and season is just a year, try "YYYY-YYYY+1" format
            if not season_obj and season.isdigit() and len(season) == 4:
                next_year = str(int(season) + 1)
                formatted_season = f"{season}-{next_year}"
                print(f"Trying formatted season: {formatted_season}")
                season_obj = self.db.query(Season).filter(
                    Season.name == formatted_season
                ).first()
            
            # Still no match? Try LIKE as fallback
            if not season_obj:
                print(f"Trying LIKE pattern: {season}-%")
                season_obj = self.db.query(Season).filter(
                    Season.name.like(f"{season}-%")
                ).order_by(Season.name.desc()).first()
            
            if season_obj:
                print(f"✅ Found season: {season_obj.name} (id: {season_obj.id})")
                roster_query = roster_query.filter(PlayerRoster.season_id == season_obj.id)
            else:
                print(f"⚠️ WARNING: No season found matching '{season}', will return most recent roster")
        
        # Get most recent roster if multiple found
        roster = roster_query.order_by(PlayerRoster.season_id.desc()).first()
        
        if not roster:
            print(f"❌ No roster entry found for player: {player_id}")
            return None
        
        print(f"✅ Found roster entry with team_id: {roster.team_id} and school_id: {roster.school_id}")
        
        # If we have a school_id, use that to find the team
        if roster.school_id:
            school = self.db.query(SchoolInfo).filter(
                SchoolInfo.id == roster.school_id
            ).first()
            
            if school:
                print(f"Found school: {school.name}")
                
                # Default to checking man_id first, then woman_id
                if school.man_id:
                    team = self.db.query(Team).filter(
                        func.lower(Team.id) == school.man_id.lower()
                    ).first()
                    
                    if team:
                        print(f"Found men's team: {team.name}")
                        return {
                            "team_id": team.id,
                            "team_name": team.name,
                            "abbreviation": team.abbreviation,
                            "conference": team.conference,
                            "gender": team.gender
                        }
                
                if school.woman_id:
                    team = self.db.query(Team).filter(
                        func.lower(Team.id) == school.woman_id.lower()
                    ).first()
                    
                    if team:
                        print(f"Found women's team: {team.name}")
                        return {
                            "team_id": team.id,
                            "team_name": team.name,
                            "abbreviation": team.abbreviation,
                            "conference": team.conference,
                            "gender": team.gender
                        }
                
                # Fall back to school info if no team is found
                return {
                    "school_id": school.id,
                    "team_name": school.name,
                    "team_id": roster.team_id or school.man_id or school.woman_id,
                    "conference": school.conference
                }
        
        # Try direct team lookup if no school or school lookup failed
        if roster.team_id:
            team = self.db.query(Team).filter(
                func.lower(Team.id) == roster.team_id.lower()
            ).first()
            
            if team:
                print(f"Found team directly: {team.name}")
                return {
                    "team_id": team.id,
                    "team_name": team.name,
                    "abbreviation": team.abbreviation,
                    "conference": team.conference,
                    "gender": team.gender
                }
        
        return None

    def get_player_stats(self, player_id: str, season: Optional[str] = None):
        """Calculate player stats from match results"""
        if not player_id:
            return None
        
        # Debug
        print(f"Calculating stats for player {player_id}")
        
        # Use PlayerMatchParticipant to get match data with win/loss info
        query = self.db.query(
            PlayerMatchParticipant
        ).join(
            PlayerMatch, 
            PlayerMatchParticipant.match_id == PlayerMatch.id
        ).filter(
            PlayerMatchParticipant.person_id == player_id
        )
        
        # Add season filter if provided
        if season:
            year = int(season)
            season_start = datetime(year, 8, 1)
            season_end = datetime(year + 1, 7, 31)
            query = query.filter(
                PlayerMatch.start_time.between(season_start, season_end)
            )
        
        participants = query.all()
        
        # Track stats
        singles_wins = 0
        singles_losses = 0
        doubles_wins = 0
        doubles_losses = 0
        
        for participant in participants:
            match = participant.match
            
            # Skip if not a valid match type
            if not match or not match.match_type:
                continue
                
            # Count win or loss based on match type
            if match.match_type == 'SINGLES':
                if participant.is_winner:
                    singles_wins += 1
                else:
                    singles_losses += 1
            elif match.match_type == 'DOUBLES':
                if participant.is_winner:
                    doubles_wins += 1
                else:
                    doubles_losses += 1
        
        # Calculate percentages, avoiding division by zero
        singles_total = singles_wins + singles_losses
        singles_win_pct = (singles_wins / singles_total) * 100 if singles_total > 0 else 0
        
        doubles_total = doubles_wins + doubles_losses
        doubles_win_pct = (doubles_wins / doubles_total) * 100 if doubles_total > 0 else 0
        
        # Get WTN ratings
        wtn_data = self.get_player_wtn(player_id)
        print(wtn_data)
        singles_wtn = None
        doubles_wtn = None
        
        if wtn_data:
            for wtn in wtn_data:
                if isinstance(wtn, dict):
                    if wtn.get("wtn_type") == "SINGLES":
                        singles_wtn = wtn.get("tennis_number")
                    elif wtn.get("wtn_type") == "DOUBLES":
                        doubles_wtn = wtn.get("tennis_number")
        
        print(f"Stats calculation complete: Singles {singles_wins}-{singles_losses}, Doubles {doubles_wins}-{doubles_losses}")
        
        return {
            "singles_wins": singles_wins,
            "singles_losses": singles_losses,
            "singles_win_pct": singles_win_pct,
            "doubles_wins": doubles_wins,
            "doubles_losses": doubles_losses,
            "doubles_win_pct": doubles_win_pct,
            "wtn_singles": singles_wtn,
            "wtn_doubles": doubles_wtn
        }

    def get_player_positions(self, player_id: str, season: Optional[str] = None):
        """Get data about positions played by the player"""
        if not player_id:
            return {"singles": [], "doubles": []}
        
        print(f"Getting position data for player {player_id}")
        
        # Query match lineups directly 
        lineups_query = self.db.query(MatchLineup).filter(
            or_(
                MatchLineup.side1_player1_id == player_id,
                MatchLineup.side1_player2_id == player_id,
                MatchLineup.side2_player1_id == player_id,
                MatchLineup.side2_player2_id == player_id
            )
        )
        
        # Add season filter if provided
        if season:
            year = int(season)
            season_start = datetime(year, 8, 1)
            season_end = datetime(year + 1, 7, 31)
            
            # Join with Match to filter by date
            lineups_query = lineups_query.join(
                Match, 
                MatchLineup.match_id == Match.id
            ).filter(
                Match.start_date.between(season_start, season_end)
            )
        
        lineups = lineups_query.all()
        print(f"Found {len(lineups)} lineup entries")
        
        # Process lineups to get position data
        singles_positions = {}
        doubles_positions = {}
        
        for lineup in lineups:
            match_type = lineup.match_type
            position = lineup.position
            
            # Determine if player was on side1 or side2
            player_on_side1 = (lineup.side1_player1_id == player_id or 
                            lineup.side1_player2_id == player_id)
            
            # Determine if player won
            won = False
            if player_on_side1:
                won = lineup.side1_won
            else:
                won = lineup.side2_won
            
            if match_type == 'SINGLES':
                if position not in singles_positions:
                    singles_positions[position] = {"position": position, "matches_count": 0, "wins": 0, "losses": 0}
                singles_positions[position]["matches_count"] += 1
                if won:
                    singles_positions[position]["wins"] += 1
                else:
                    singles_positions[position]["losses"] += 1
            elif match_type == 'DOUBLES':
                if position not in doubles_positions:
                    doubles_positions[position] = {"position": position, "matches_count": 0, "wins": 0, "losses": 0}
                doubles_positions[position]["matches_count"] += 1
                if won:
                    doubles_positions[position]["wins"] += 1
                else:
                    doubles_positions[position]["losses"] += 1
        
        print(f"Positions data: Singles positions: {len(singles_positions)}, Doubles positions: {len(doubles_positions)}")
        
        return {
            "singles": list(singles_positions.values()),
            "doubles": list(doubles_positions.values())
        }

    def _did_player_win(self, match, player_id):
        """Helper to determine if player won a match"""
        # Check if player was on side1 or side2
        side1 = (match.get('side1_player1_id') == player_id or 
                match.get('side1_player2_id') == player_id)
        
        if side1:
            return match.get('side1_won', False)
        else:
            return match.get('side2_won', False)
        
    def get_player_match_results(self, player_id: str, season: Optional[str] = None):
        """Get enhanced match results for a player with optimization"""
        if not player_id:
            return []
        
        print(f"Getting detailed match results for player {player_id}")
        
        # Query player match participations with all necessary joins in one go
        query = self.db.query(
            PlayerMatchParticipant, PlayerMatch
        ).join(
            PlayerMatch,
            PlayerMatchParticipant.match_id == PlayerMatch.id
        ).filter(
            PlayerMatchParticipant.person_id == player_id
        )
        
        # Add season filter if provided
        if season:
            try:
                year = int(season)
                season_start = datetime(year, 8, 1)
                season_end = datetime(year + 1, 7, 31)
                query = query.filter(
                    PlayerMatch.start_time.between(season_start, season_end)
                )
            except ValueError:
                print(f"Invalid season format: {season}")
        
        # Execute query
        results_query = query.all()
        print(f"Found {len(results_query)} match participations")
        
        # Batch collect all match IDs to fetch related data in bulk
        match_ids = [r[1].id for r in results_query]
        
        # Fetch all participants for these matches in a single query
        all_participants = self.db.query(PlayerMatchParticipant).filter(
            PlayerMatchParticipant.match_id.in_(match_ids)
        ).all()
        
        # Group participants by match_id
        participants_by_match = {}
        for p in all_participants:
            if p.match_id not in participants_by_match:
                participants_by_match[p.match_id] = []
            participants_by_match[p.match_id].append(p)
        
        # Fetch all lineups for these matches in a single query (if possible)
        # Note: We're assuming match_identifier matches with MatchLineup.match_id
        match_identifiers = []
        for r in results_query:
            if r[1].match_identifier:
                match_identifiers.append(r[1].match_identifier)
        
        all_lineups = {}
        if match_identifiers:
            lineup_query = self.db.query(MatchLineup).filter(
                MatchLineup.match_id.in_(match_identifiers)
            ).all()
            
            for lineup in lineup_query:
                all_lineups[lineup.match_id] = lineup
        
        # Create results
        results = []
        for participant, match in results_query:
            # Skip invalid matches
            if not match:
                continue
            
            # Get all participants for this match
            match_participants = participants_by_match.get(match.id, [])
            
            # Separate opponent participants
            opponent_participants = [
                p for p in match_participants 
                if p.side_number != participant.side_number
            ]
            
            # Get partner (for doubles)
            partner = None
            if match.match_type == 'DOUBLES':
                for p in match_participants:
                    if (p.side_number == participant.side_number and 
                        p.person_id != participant.person_id):
                        partner = p
                        break
            
            # Get team information
            opponent_team_id = None
            if opponent_participants and opponent_participants[0].team_id:
                opponent_team_id = opponent_participants[0].team_id
            
            # Get opponent names
            opponent_name1 = ""
            opponent_name2 = None
            
            if opponent_participants:
                if len(opponent_participants) > 0:
                    opponent_name1 = f"{opponent_participants[0].given_name} {opponent_participants[0].family_name}"
                if len(opponent_participants) > 1:
                    opponent_name2 = f"{opponent_participants[1].given_name} {opponent_participants[1].family_name}"
            
            # Get partner name
            partner_name = None
            if partner:
                partner_name = f"{partner.given_name} {partner.family_name}"
            
            print(vars(match))  # Shows all attributes as a dictionary
            # Get position from lineup
            position = 0
            if match.match_identifier in all_lineups:
                position = all_lineups[match.match_identifier].position
            
            # Create result object
            result = {
                "id": f"{match.id}_{participant.id}",
                "match_id": match.match_identifier,
                "date": match.start_time,
                "opponent_name": opponent_name1,
                "opponent_team_id": opponent_team_id,
                "is_home": participant.side_number == "SIDE1",  # Assuming SIDE1 is home
                "match_type": match.match_type,
                "position": match.collection_position if match.collection_position is not None else position,
                "score": match.score_string,
                "won": participant.is_winner,
                "partner_name": partner_name,
                "opponent_name1": opponent_name1,
                "opponent_name2": opponent_name2
            }
            
            results.append(result)
        
        # Limit results if there are too many
        # if len(results) > 50:
        #     print(f"Limiting results from {len(results)} to 50")
        #     results = results[:50]
        
        print(f"Returning {len(results)} match results")
        return results
    
    def search_all_players(self, query: str = None, gender: str = None, season_name: str = None):
        """Search for players using raw SQL to query the player_search_view"""
        print(f"DEBUG: Service method called with params: query={query}, gender={gender}, season_name={season_name}")
        
        try:
            # Build the SQL query with DISTINCT ON to get only one row per player
            sql = """
            SELECT DISTINCT ON (person_id) 
                person_id, tennis_id, first_name, last_name, avatar_url,
                team_id, team_name, gender, conference, division,
                season_name, season_id, school_name, school_id,
                wtn_singles, wtn_doubles
            FROM player_search_view 
            WHERE 1=1
            AND active = TRUE
            """
            params = {}
            
            # Apply text search filter if provided
            if query and len(query) >= 2:
                sql += """ AND (
                    LOWER(first_name) LIKE :search_term OR
                    LOWER(last_name) LIKE :search_term OR
                    LOWER(team_name) LIKE :search_term OR
                    LOWER(school_name) LIKE :search_term OR
                    LOWER(CONCAT(first_name, ' ', last_name)) LIKE :search_term
                )"""
                params['search_term'] = f"%{query.lower()}%"
            
            # Apply gender filter if provided
            if gender:
                if gender.upper() in ['MALE', 'M']:
                    sql += " AND (UPPER(gender) = 'MALE' OR UPPER(gender) = 'M')"
                elif gender.upper() in ['FEMALE', 'F']:
                    sql += " AND (UPPER(gender) = 'FEMALE' OR UPPER(gender) = 'F')"
                else:
                    sql += " AND UPPER(gender) = :gender"
                    params['gender'] = gender.upper()
            
            # Apply season filter if provided  
            if season_name:
                sql += " AND (season_name = :season_name OR season_name LIKE :season_pattern)"
                params['season_name'] = season_name
                params['season_pattern'] = f"%{season_name}%"
            
            # Order by person_id to support DISTINCT ON, then by season to get most recent
            sql += " ORDER BY person_id, season_name DESC NULLS LAST LIMIT 500"
            
            print(f"DEBUG: Executing SQL: {sql}")
            print(f"DEBUG: With params: {params}")
            
            # Execute raw SQL query
            result = self.db.execute(text(sql), params).fetchall()
            
            print(f"DEBUG: Query returned {len(result)} rows")
            
            # Process results
            player_results = []
            for row in result:
                row_dict = dict(row._mapping)
                player_results.append(row_dict)
            
            print(f"DEBUG: Returning {len(player_results)} processed results")
            return player_results
            
        except Exception as e:
            print(f"ERROR: Exception in search_all_players: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise

    def get_player_seasons(self, player_id: str, include_current: bool = True):
        """
        Get seasons where the player has data (roster, WTN, or matches).
        Optionally always include the current active season regardless of data.
        
        Args:
            player_id: The player's person_id
            include_current: If True, always include the current/active season
        
        Returns:
            List of season dictionaries with id, name, status
        """
        if not player_id:
            return []
        
        upper_player_id = player_id.upper()
        
        # Collect season IDs where player has data
        season_ids = set()
        
        # Get seasons where player has roster entries
        roster_season_ids = self.db.query(PlayerRoster.season_id).filter(
            func.upper(PlayerRoster.person_id) == upper_player_id
        ).distinct().all()
        season_ids.update([s[0] for s in roster_season_ids if s[0]])
        
        # Get seasons where player has WTN data
        wtn_season_ids = self.db.query(PlayerWTN.season_id).filter(
            func.upper(PlayerWTN.person_id) == upper_player_id
        ).distinct().all()
        season_ids.update([s[0] for s in wtn_season_ids if s[0]])
        
        # Get seasons where player has match data
        # Since PlayerMatch doesn't have season_id, we need to:
        # 1. Get all match dates for this player
        # 2. Map them to seasons based on date ranges
        player_matches = self.db.query(PlayerMatch.start_time).join(
            PlayerMatchParticipant, PlayerMatch.id == PlayerMatchParticipant.match_id
        ).filter(
            func.upper(PlayerMatchParticipant.person_id) == upper_player_id
        ).all()
        
        # Get all seasons to map dates to season IDs
        all_seasons = self.db.query(Season).all()
        
        # Map match dates to seasons
        for match in player_matches:
            if match.start_time:
                match_date = match.start_time
                # Convert to date for comparison
                match_date_only = match_date.date() if hasattr(match_date, 'date') else match_date
                
                # Find which season this match belongs to
                for season in all_seasons:
                    if season.start_date and season.end_date:
                        # Ensure both sides are date objects for comparison
                        season_start = season.start_date if isinstance(season.start_date, type(match_date_only)) else season.start_date.date()
                        season_end = season.end_date if isinstance(season.end_date, type(match_date_only)) else season.end_date.date()
                        
                        if season_start <= match_date_only <= season_end:
                            season_ids.add(season.id)
                            break
                    # Fallback: use academic year logic (Aug 1 to July 31)
                    elif season.name:
                        try:
                            # Parse season name like "2024-2025" or "2024"
                            season_year = int(season.name.split('-')[0])
                            season_start = datetime(season_year, 8, 1).date()
                            season_end = datetime(season_year + 1, 7, 31).date()
                            if season_start <= match_date_only <= season_end:
                                season_ids.add(season.id)
                                break
                        except (ValueError, IndexError):
                            continue
        
        # If include_current is True, add the active season
        if include_current:
            active_season = self.db.query(Season).filter(
                Season.status == 'ACTIVE'
            ).first()
            
            if active_season:
                season_ids.add(active_season.id)
        
        # Fetch all seasons that are in our season_ids set
        if not season_ids:
            # If no seasons found but include_current is True, return just active season
            if include_current:
                active_season = self.db.query(Season).filter(
                    Season.status == 'ACTIVE'
                ).first()
                if active_season:
                    return [{
                        "id": active_season.id,
                        "name": active_season.name,
                        "status": active_season.status,
                        "start_date": str(active_season.start_date) if active_season.start_date else None,
                        "end_date": str(active_season.end_date) if active_season.end_date else None
                    }]
            return []
        
        seasons = self.db.query(Season).filter(
            Season.id.in_(season_ids)
        ).order_by(Season.name.desc()).all()
        
        # Convert to dictionaries
        return [
            {
                "id": season.id,
                "name": season.name,
                "status": season.status,
                "start_date": str(season.start_date) if season.start_date else None,
                "end_date": str(season.end_date) if season.end_date else None
            }
            for season in seasons
        ]