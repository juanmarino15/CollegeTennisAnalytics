# services/tournament_players_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from typing import List, Optional, Dict, Any
from datetime import datetime
from models.models import TournamentPlayer, Tournament

class TournamentPlayersService:
    def __init__(self, db: Session):
        self.db = db

    def get_tournament_players(
        self,
        tournament_id: str,
        gender: Optional[str] = None,
        event_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Get all players registered for a specific tournament with filters"""
        
        # First check if tournament exists
        tournament = self.db.query(Tournament).filter(
            Tournament.tournament_id == tournament_id
        ).first()
        
        if not tournament:
            return None
        
        # Build query for registrations
        query = self.db.query(TournamentPlayer).filter(
            TournamentPlayer.tournament_id == tournament_id
        )
        
        # Apply filters
        if gender:
            query = query.filter(TournamentPlayer.gender == gender.upper())
        
        if state:
            query = query.filter(TournamentPlayer.state == state.upper())
        
        if event_type:
            if event_type.lower() == 'singles':
                query = query.filter(
                    TournamentPlayer.events_participating.contains('singles')
                )
            elif event_type.lower() == 'doubles':
                query = query.filter(
                    TournamentPlayer.events_participating.contains('doubles')
                )
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination and get results
        registrations = query.order_by(
            TournamentPlayer.last_name,
            TournamentPlayer.first_name
        ).offset(offset).limit(limit).all()
        
        return {
            "total_items": total_count,
            "tournament_id": tournament_id,
            "tournament_name": tournament.name,
            "players": registrations
        }

    def get_tournament_player_stats(self, tournament_id: str) -> Optional[Dict[str, Any]]:
        """Get statistics about players in a tournament"""
        
        # Check if tournament exists
        tournament = self.db.query(Tournament).filter(
            Tournament.tournament_id == tournament_id
        ).first()
        
        if not tournament:
            return None
        
        # Get all registrations for this tournament
        registrations = self.db.query(TournamentPlayer).filter(
            TournamentPlayer.tournament_id == tournament_id
        ).all()
        
        if not registrations:
            return {
                "total_registrations": 0,
                "unique_players": 0,
                "singles_players": 0,
                "doubles_players": 0,
                "both_events_players": 0,
                "gender_breakdown": {},
                "state_breakdown": {}
            }
        
        # Calculate statistics
        total_registrations = len(registrations)
        unique_players = len(set(reg.player_id for reg in registrations))
        
        singles_players = 0
        doubles_players = 0
        both_events_players = 0
        gender_breakdown = {}
        state_breakdown = {}
        
        for reg in registrations:
            # Event participation
            events = reg.events_participating.split(',') if reg.events_participating else []
            has_singles = 'singles' in events
            has_doubles = 'doubles' in events
            
            if has_singles and has_doubles:
                both_events_players += 1
            elif has_singles:
                singles_players += 1
            elif has_doubles:
                doubles_players += 1
            
            # Gender breakdown
            gender = reg.gender or 'UNKNOWN'
            gender_breakdown[gender] = gender_breakdown.get(gender, 0) + 1
            
            # State breakdown
            state = reg.state or 'UNKNOWN'
            state_breakdown[state] = state_breakdown.get(state, 0) + 1
        
        return {
            "total_registrations": total_registrations,
            "unique_players": unique_players,
            "singles_players": singles_players,
            "doubles_players": doubles_players,
            "both_events_players": both_events_players,
            "gender_breakdown": gender_breakdown,
            "state_breakdown": state_breakdown
        }

    def get_player_tournaments(
        self,
        player_id: str,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get all tournaments a player is registered for"""
        
        # Build query joining registrations with tournaments
        query = self.db.query(
            TournamentPlayer,
            Tournament
        ).join(
            Tournament,
            TournamentPlayer.tournament_id == Tournament.tournament_id
        ).filter(
            TournamentPlayer.player_id == player_id
        )
        
        # Apply date filters
        if from_date:
            query = query.filter(Tournament.start_date_time >= from_date)
        if to_date:
            query = query.filter(Tournament.start_date_time <= to_date)
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination and ordering
        results = query.order_by(
            Tournament.start_date_time.desc()
        ).offset(offset).limit(limit).all()
        
        # Format results
        tournaments = []
        for registration, tournament in results:
            tournaments.append({
                "tournament": {
                    "tournament_id": tournament.tournament_id,
                    "name": tournament.name,
                    "start_date_time": tournament.start_date_time,
                    "end_date_time": tournament.end_date_time,
                    "location_name": tournament.location_name,
                    "primary_location_town": tournament.primary_location_town,
                    "primary_location_county": tournament.primary_location_county,
                },
                "registration": {
                    "events_participating": registration.events_participating,
                    "singles_event_id": registration.singles_event_id,
                    "doubles_event_id": registration.doubles_event_id,
                    "player2_id": registration.player2_id,
                    "player2_first_name": registration.player2_first_name,
                    "player2_last_name": registration.player2_last_name,
                }
            })
        
        return {
            "total_items": total_count,
            "player_id": player_id,
            "tournaments": tournaments
        }

    def search_tournament_players(
        self,
        player_name: Optional[str] = None,
        state: Optional[str] = None,
        gender: Optional[str] = None,
        tournament_name: Optional[str] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Search for tournament players across all tournaments"""
        
        # Build query joining registrations with tournaments
        query = self.db.query(
            TournamentPlayer,
            Tournament
        ).join(
            Tournament,
            TournamentPlayer.tournament_id == Tournament.tournament_id
        )
        
        # Apply filters
        if player_name:
            # Search in both player_name and combined first/last name
            search_term = f"%{player_name.lower()}%"
            query = query.filter(
                or_(
                    func.lower(TournamentPlayer.player_name).contains(search_term),
                    func.lower(
                        func.concat(
                            TournamentPlayer.first_name, 
                            ' ', 
                            TournamentPlayer.last_name
                        )
                    ).contains(search_term)
                )
            )
        
        if state:
            query = query.filter(TournamentPlayer.state == state.upper())
        
        if gender:
            query = query.filter(TournamentPlayer.gender == gender.upper())
        
        if tournament_name:
            search_term = f"%{tournament_name.lower()}%"
            query = query.filter(func.lower(Tournament.name).contains(search_term))
        
        if from_date:
            query = query.filter(Tournament.start_date_time >= from_date)
        
        if to_date:
            query = query.filter(Tournament.start_date_time <= to_date)
        
        if event_type:
            if event_type.lower() == 'singles':
                query = query.filter(
                    TournamentPlayer.events_participating.contains('singles')
                )
            elif event_type.lower() == 'doubles':
                query = query.filter(
                    TournamentPlayer.events_participating.contains('doubles')
                )
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination and ordering
        results = query.order_by(
            Tournament.start_date_time.desc(),
            TournamentPlayer.last_name,
            TournamentPlayer.first_name
        ).offset(offset).limit(limit).all()
        
        # Format results
        players = []
        for registration, tournament in results:
            players.append({
                "registration": {
                    "id": registration.id,
                    "player_id": registration.player_id,
                    "first_name": registration.first_name,
                    "last_name": registration.last_name,
                    "player_name": registration.player_name,
                    "gender": registration.gender,
                    "city": registration.city,
                    "state": registration.state,
                    "events_participating": registration.events_participating,
                    "player2_id": registration.player2_id,
                    "player2_first_name": registration.player2_first_name,
                    "player2_last_name": registration.player2_last_name,
                },
                "tournament": {
                    "tournament_id": tournament.tournament_id,
                    "name": tournament.name,
                    "start_date_time": tournament.start_date_time,
                    "end_date_time": tournament.end_date_time,
                    "location_name": tournament.location_name,
                    "primary_location_town": tournament.primary_location_town,
                    "primary_location_county": tournament.primary_location_county,
                }
            })
        
        return {
            "total_items": total_count,
            "players": players
        }

    def get_doubles_partnerships(
        self,
        tournament_id: Optional[str] = None,
        player_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get doubles partnerships (player1/player2 pairs) from tournament registrations"""
        
        # Build base query for registrations with player2
        query = self.db.query(TournamentPlayer).filter(
            TournamentPlayer.player2_id.isnot(None)
        )
        
        # Apply filters
        if tournament_id:
            query = query.filter(TournamentPlayer.tournament_id == tournament_id)
        
        if player_id:
            query = query.filter(
                or_(
                    TournamentPlayer.player_id == player_id,
                    TournamentPlayer.player2_id == player_id
                )
            )
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        registrations = query.offset(offset).limit(limit).all()
        
        # Format partnerships (avoiding duplicates)
        partnerships = []
        seen_partnerships = set()
        
        for reg in registrations:
            # Create a consistent partnership key (sorted player IDs)
            partnership_key = tuple(sorted([reg.player_id, reg.player2_id]))
            
            if partnership_key not in seen_partnerships:
                seen_partnerships.add(partnership_key)
                
                partnerships.append({
                    "tournament_id": reg.tournament_id,
                    "doubles_event_id": reg.doubles_event_id,
                    "player1": {
                        "player_id": reg.player_id,
                        "first_name": reg.first_name,
                        "last_name": reg.last_name,
                        "player_name": reg.player_name
                    },
                    "player2": {
                        "player_id": reg.player2_id,
                        "first_name": reg.player2_first_name,
                        "last_name": reg.player2_last_name,
                        "player_name": f"{reg.player2_first_name} {reg.player2_last_name}" if reg.player2_first_name and reg.player2_last_name else None
                    }
                })
        
        return {
            "total_items": len(partnerships),
            "partnerships": partnerships
        }