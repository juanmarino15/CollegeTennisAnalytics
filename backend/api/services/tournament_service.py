# api/services/tournament_service.py
from typing import List, Dict, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func
from datetime import datetime, timedelta
from models.models import Tournament, TournamentEvent, Match

class TournamentService:
    def __init__(self, db: Session):
        self.db = db

    def get_tournaments_and_matches(self, 
                                  match_type: str = "all",  # "all", "dual", "tournaments"
                                  from_date: Optional[datetime] = None,
                                  to_date: Optional[datetime] = None,
                                  size: int = 25,
                                  offset: int = 0,
                                  latitude: float = 0,
                                  longitude: float = 0,
                                  sort_key: str = "date") -> Dict[str, Any]:
        """
        Get tournaments and dual matches based on filter criteria
        """
        
        if not from_date:
            from_date = datetime.now()
        
        results = []
        total_count = 0
        
        if match_type in ["all", "dual"]:
            # Get dual matches (team vs team matches)
            dual_matches = self._get_dual_matches(from_date, to_date, size if match_type == "dual" else size // 2, offset)
            results.extend(dual_matches["matches"])
            if match_type == "dual":
                total_count = dual_matches["total"]
        
        if match_type in ["all", "tournaments"]:
            # Get tournament events
            tournaments = self._get_tournaments(from_date, to_date, size if match_type == "tournaments" else size // 2, offset)
            results.extend(tournaments["tournaments"])
            if match_type == "tournaments":
                total_count = tournaments["total"]
        
        if match_type == "all":
            # Combine and sort by date
            results = sorted(results, key=lambda x: x.get("start_date", datetime.min))
            total_count = len(results)
            
            # Apply pagination to combined results
            results = results[offset:offset + size]
        
        return {
            "total": total_count,
            "searchResults": [{"distance": 0, "item": item} for item in results]
        }

    def _get_dual_matches(self, from_date: datetime, to_date: Optional[datetime], size: int, offset: int) -> Dict[str, Any]:
        """Get dual matches (team vs team matches)"""
        
        query = self.db.query(Match).filter(
            Match.start_date >= from_date,
            Match.completed == False  # Only upcoming matches
        )
        
        if to_date:
            query = query.filter(Match.start_date <= to_date)
        
        # Count total matches
        total = query.count()
        
        # Get paginated results
        matches = query.order_by(Match.start_date).offset(offset).limit(size).all()
        
        # Format matches to match tournament API structure
        formatted_matches = []
        for match in matches:
            # Get match events (lineup)
            events = self._get_match_events(match.id)
            
            formatted_match = {
                "id": match.id,
                "identificationCode": f"DUAL-{match.id[:8]}",
                "name": f"{match.home_team.name if match.home_team else 'Home'} vs {match.away_team.name if match.away_team else 'Away'}",
                "startDateTime": match.start_date.isoformat() + "Z" if match.start_date else None,
                "endDateTime": match.start_date.isoformat() + "Z" if match.start_date else None,  # Dual matches typically same day
                "timeZone": match.timezone or "UTC",
                "isCancelled": False,
                "url": f"/matches/{match.id}",
                "events": events,
                "level": {
                    "id": "dual-match",
                    "name": "Dual Match",
                    "branding": None
                },
                "levelCategories": [{"name": "college"}],
                "location": {
                    "id": "dual-location",
                    "name": "Dual Match Location",
                    "geo": {"latitude": 0, "longitude": 0}
                },
                "organization": {
                    "id": match.home_team_id if match.home_team_id else "unknown",
                    "name": match.home_team.name if match.home_team else "Unknown Team",
                    "conference": match.home_team.conference if match.home_team else None,
                    "division": match.home_team.division if match.home_team else None
                },
                "primaryLocation": {
                    "town": "Unknown",
                    "county": "Unknown"
                },
                # Custom fields to identify as dual match
                "_isDualMatch": True,
                "_matchType": "DUAL_MATCH"
            }
            formatted_matches.append(formatted_match)
        
        return {
            "matches": formatted_matches,
            "total": total
        }

    def _get_tournaments(self, from_date: datetime, to_date: Optional[datetime], size: int, offset: int) -> Dict[str, Any]:
        """Get tournament events"""
        
        query = self.db.query(Tournament).filter(
            Tournament.start_date_time >= from_date,
            Tournament.is_dual_match == False  # Only tournaments, not dual matches
        )
        
        if to_date:
            query = query.filter(Tournament.start_date_time <= to_date)
        
        # Count total tournaments
        total = query.count()
        
        # Get paginated results
        tournaments = query.order_by(Tournament.start_date_time).offset(offset).limit(size).all()
        
        # Format tournaments to match API structure
        formatted_tournaments = []
        for tournament in tournaments:
            # Get tournament events
            events = self._get_tournament_events(tournament.tournament_id)
            level_categories = self._get_tournament_level_categories(tournament.tournament_id)
            
            formatted_tournament = {
                "id": tournament.tournament_id,
                "identificationCode": tournament.identification_code,
                "name": tournament.name,
                "image": tournament.image,
                "startDateTime": tournament.start_date_time.isoformat() + "Z" if tournament.start_date_time else None,
                "endDateTime": tournament.end_date_time.isoformat() + "Z" if tournament.end_date_time else None,
                "timeZone": tournament.time_zone,
                "isCancelled": tournament.is_cancelled,
                "url": tournament.url,
                "events": events,
                "level": {
                    "id": tournament.level_id,
                    "name": tournament.level_name,
                    "branding": tournament.level_branding
                },
                "levelCategories": level_categories,
                "location": {
                    "id": tournament.location_id,
                    "name": tournament.location_name,
                    "geo": {
                        "latitude": tournament.geo_latitude,
                        "longitude": tournament.geo_longitude
                    }
                },
                "organization": {
                    "id": tournament.organization_id,
                    "name": tournament.organization_name,
                    "conference": tournament.organization_conference,
                    "division": tournament.organization_division,
                    "urlSegment": tournament.organization_url_segment
                },
                "primaryLocation": {
                    "address1": tournament.primary_location_address1,
                    "address2": tournament.primary_location_address2,
                    "address3": tournament.primary_location_address3,
                    "town": tournament.primary_location_town,
                    "county": tournament.primary_location_county,
                    "postcode": tournament.primary_location_postcode
                },
                "registrationRestrictions": {
                    "entriesOpenDateTime": tournament.entries_open_date_time.isoformat() + "Z" if tournament.entries_open_date_time else None,
                    "entriesCloseDateTime": tournament.entries_close_date_time.isoformat() + "Z" if tournament.entries_close_date_time else None,
                    "secondsUntilEntriesClose": tournament.seconds_until_entries_close,
                    "secondsUntilEntriesOpen": tournament.seconds_until_entries_open,
                    "timeZone": tournament.registration_time_zone
                },
                # Custom fields to identify as tournament
                "_isDualMatch": False,
                "_matchType": "TOURNAMENT"
            }
            formatted_tournaments.append(formatted_tournament)
        
        return {
            "tournaments": formatted_tournaments,
            "total": total
        }

    def _get_match_events(self, match_id: str) -> List[Dict[str, Any]]:
        """Get events for a dual match (singles and doubles lineups)"""
        from models.models import MatchLineup
        
        lineups = self.db.query(MatchLineup).filter_by(match_id=match_id).all()
        
        events = []
        
        # Group lineups by match type to create events
        singles_positions = [lineup for lineup in lineups if lineup.match_type == 'SINGLES']
        doubles_positions = [lineup for lineup in lineups if lineup.match_type == 'DOUBLES']
        
        # Create singles event if positions exist
        if singles_positions:
            events.append({
                "id": f"{match_id}-singles",
                "division": {
                    "gender": "mixed",  # Could be determined from team info
                    "eventType": "singles"
                }
            })
        
        # Create doubles event if positions exist
        if doubles_positions:
            events.append({
                "id": f"{match_id}-doubles", 
                "division": {
                    "gender": "mixed",  # Could be determined from team info
                    "eventType": "doubles"
                }
            })
        
        return events

    def _get_tournament_events(self, tournament_id: str) -> List[Dict[str, Any]]:
        """Get events for a tournament from the simplified tournament_events table"""
        events = self.db.query(TournamentEvent).filter_by(tournament_id=tournament_id).all()
        
        formatted_events = []
        for event in events:
            formatted_event = {
                "id": event.event_id,
                "division": {
                    "gender": event.gender,
                    "eventType": event.event_type
                }
            }
            formatted_events.append(formatted_event)
        
        return formatted_events

    def _get_tournament_level_categories(self, tournament_id: str) -> List[Dict[str, Any]]:
        """Get level categories for a tournament - simplified version"""
        # Since we simplified the model, we'll derive this from the tournament level_category field
        tournament = self.db.query(Tournament).filter_by(tournament_id=tournament_id).first()
        
        if tournament and tournament.level_category:
            return [{"name": tournament.level_category}]
        
        return [{"name": "college"}]  # Default

    def search_by_type(self, search_type: str, **kwargs) -> Dict[str, Any]:
        """
        Search specifically for dual matches or tournaments
        
        Args:
            search_type: "dual" or "tournaments"
            **kwargs: Additional search parameters (date filters, pagination, etc.)
        """
        
        if search_type == "dual":
            return self.get_tournaments_and_matches(match_type="dual", **kwargs)
        elif search_type == "tournaments":
            return self.get_tournaments_and_matches(match_type="tournaments", **kwargs)
        else:
            raise ValueError("search_type must be 'dual' or 'tournaments'")

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about dual matches vs tournaments"""
        
        # Count dual matches
        dual_count = self.db.query(Match).filter(Match.completed == False).count()
        
        # Count tournaments
        tournament_count = self.db.query(Tournament).filter(
            Tournament.is_dual_match == False,
            Tournament.start_date_time >= datetime.now()
        ).count()
        
        # Count tournament events by type
        singles_events = self.db.query(TournamentEvent).filter(
            TournamentEvent.event_type == 'singles'
        ).count()
        
        doubles_events = self.db.query(TournamentEvent).filter(
            TournamentEvent.event_type == 'doubles'
        ).count()
        
        return {
            "dual_matches": dual_count,
            "tournaments": tournament_count,
            "tournament_singles_events": singles_events,
            "tournament_doubles_events": doubles_events,
            "total": dual_count + tournament_count
        }

    def get_tournament_events_by_type(self, tournament_id: str, gender: str = None, event_type: str = None) -> List[TournamentEvent]:
        """Get tournament events filtered by gender and/or event type"""
        query = self.db.query(TournamentEvent).filter_by(tournament_id=tournament_id)
        
        if gender:
            query = query.filter(TournamentEvent.gender == gender)
        
        if event_type:
            query = query.filter(TournamentEvent.event_type == event_type)
        
        return query.all()
def get_tournament_with_events(self, tournament_id: str) -> Optional[Dict[str, Any]]:
    """Get a tournament with all its events"""
    tournament = self.db.query(Tournament).filter_by(tournament_id=tournament_id).first()
    
    if not tournament:
        return None
    
    events = self.db.query(TournamentEvent).filter_by(tournament_id=tournament_id).all()
    
    event_responses = []
    for event in events:
        event_responses.append({
            "event_id": event.event_id,
            "tournament_id": event.tournament_id,
            "gender": event.gender,
            "event_type": event.event_type,
            "created_at": event.created_at,
            "updated_at": event.updated_at
        })
    
    return {
        "tournament_id": tournament.tournament_id,
        "name": tournament.name,
        "start_date_time": tournament.start_date_time,
        "end_date_time": tournament.end_date_time,
        "location_name": tournament.location_name,
        "organization_name": tournament.organization_name,
        "events": event_responses
    }

def get_event_statistics(self) -> Dict[str, Any]:
    """Get detailed statistics about tournament events"""
    
    # Total tournaments and events
    total_tournaments = self.db.query(Tournament).filter(
        Tournament.is_dual_match == False,
        Tournament.start_date_time >= datetime.now()
    ).count()
    
    total_events = self.db.query(TournamentEvent).count()
    
    # Events by gender
    events_by_gender = {}
    gender_stats = self.db.query(
        TournamentEvent.gender, 
        func.count(TournamentEvent.event_id)
    ).group_by(TournamentEvent.gender).all()
    
    for gender, count in gender_stats:
        if gender:
            events_by_gender[gender] = count
    
    # Events by type
    events_by_type = {}
    type_stats = self.db.query(
        TournamentEvent.event_type, 
        func.count(TournamentEvent.event_id)
    ).group_by(TournamentEvent.event_type).all()
    
    for event_type, count in type_stats:
        if event_type:
            events_by_type[event_type] = count
    
    # Tournaments with both genders
    tournaments_with_both_genders = self.db.query(Tournament.tournament_id).join(
        TournamentEvent
    ).group_by(Tournament.tournament_id).having(
        func.count(func.distinct(TournamentEvent.gender)) > 1
    ).count()
    
    # Tournaments with both singles and doubles
    tournaments_with_both_types = self.db.query(Tournament.tournament_id).join(
        TournamentEvent
    ).group_by(Tournament.tournament_id).having(
        func.count(func.distinct(TournamentEvent.event_type)) > 1
    ).count()
    
    return {
        "total_tournaments": total_tournaments,
        "total_events": total_events,
        "events_by_gender": events_by_gender,
        "events_by_type": events_by_type,
        "tournaments_with_both_genders": tournaments_with_both_genders,
        "tournaments_with_both_types": tournaments_with_both_types
    }

def search_events(self, 
                gender: Optional[str] = None,
                event_type: Optional[str] = None,
                tournament_name: Optional[str] = None,
                from_date: Optional[datetime] = None,
                to_date: Optional[datetime] = None,
                size: int = 25,
                offset: int = 0) -> Dict[str, Any]:
    """Search for tournament events across all tournaments"""
    
    # Build query
    query = self.db.query(TournamentEvent).join(Tournament)
    
    # Apply filters
    if gender:
        query = query.filter(TournamentEvent.gender == gender)
    
    if event_type:
        query = query.filter(TournamentEvent.event_type == event_type)
    
    if tournament_name:
        query = query.filter(Tournament.name.ilike(f"%{tournament_name}%"))
    
    if from_date:
        query = query.filter(Tournament.start_date_time >= from_date)
    
    if to_date:
        query = query.filter(Tournament.start_date_time <= to_date)
    
    # Count total
    total = query.count()
    
    # Get paginated results
    events = query.order_by(Tournament.start_date_time).offset(offset).limit(size).all()
    
    # Format results
    results = []
    for event in events:
        tournament = self.db.query(Tournament).filter_by(tournament_id=event.tournament_id).first()
        results.append({
            "event_id": event.event_id,
            "tournament_id": event.tournament_id,
            "tournament_name": tournament.name if tournament else None,
            "tournament_start_date": tournament.start_date_time if tournament else None,
            "tournament_location": tournament.location_name if tournament else None,
            "gender": event.gender,
            "event_type": event.event_type,
            "created_at": event.created_at,
            "updated_at": event.updated_at
        })
    
    return {
        "total": total,
        "events": results,
        "filters_applied": {
            "gender": gender,
            "event_type": event_type,
            "tournament_name": tournament_name,
            "from_date": from_date.isoformat() if from_date else None,
            "to_date": to_date.isoformat() if to_date else None
        }
    }