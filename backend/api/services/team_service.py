from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from typing import List, Optional, Dict
from models import Team, TeamLogo, PlayerRoster, Player, Season

class TeamService:
    def __init__(self, db: Session):
        self.db = db

    def _team_to_dict(self, team):
        """Convert Team model to dictionary"""
        return {
            "id": team.id,
            "name": team.name,
            "abbreviation": team.abbreviation,
            "division": team.division,
            "conference": team.conference,
            "region": team.region,
            "typename": team.typename,
            "gender": team.gender
        }

    def _logo_to_dict(self, logo):
        """Convert TeamLogo model to dictionary"""
        return {
            "team_id": logo.team_id,
            "logo_data": logo.logo_data,
            "created_at": logo.created_at,
            "updated_at": logo.updated_at
        }

    def _player_to_dict(self, player):
        """Convert Player model to dictionary"""
        return {
            "person_id": player.person_id,
            "tennis_id": player.tennis_id,
            "first_name": player.first_name,
            "last_name": player.last_name,
            "avatar_url": player.avatar_url
        }

    def get_teams(self, conference: str = None):
        query = self.db.query(Team)
        if conference:
            query = query.filter(func.upper(Team.conference) == conference.upper())
        teams = query.all()
        return [self._team_to_dict(team) for team in teams]

    def get_team(self, team_id: str):
        if not team_id:
            return None
            
        # Use exact match first (faster with index)
        team = self.db.query(Team).filter(Team.id == team_id).first()
        
        # Fall back to case-insensitive if not found
        if not team:
            upper_team_id = team_id.upper()
            team = self.db.query(Team).filter(
                func.upper(Team.id) == upper_team_id
            ).first()
            
        return self._team_to_dict(team) if team else None

    def get_teams_batch(self, team_ids: List[str]) -> List[Dict]:
        """
        Fetch multiple teams in a single query.
        This is much more efficient than multiple individual queries.
        """
        if not team_ids:
            return []
        
        # Try exact match first
        teams = self.db.query(Team).filter(Team.id.in_(team_ids)).all()
        
        # If we didn't get all teams, try case-insensitive
        if len(teams) < len(team_ids):
            found_ids = {team.id for team in teams}
            missing_ids = [tid for tid in team_ids if tid not in found_ids]
            
            if missing_ids:
                upper_ids = [tid.upper() for tid in missing_ids]
                additional_teams = self.db.query(Team).filter(
                    func.upper(Team.id).in_(upper_ids)
                ).all()
                teams.extend(additional_teams)
        
        return [self._team_to_dict(team) for team in teams]

    def get_team_with_matches(self, team_id: str):
        """
        Get team with matches pre-loaded to avoid N+1 queries
        """
        if not team_id:
            return None
            
        team = self.db.query(Team).options(
            joinedload(Team.home_matches),
            joinedload(Team.away_matches)
        ).filter(Team.id == team_id).first()
        
        if not team:
            upper_team_id = team_id.upper()
            team = self.db.query(Team).options(
                joinedload(Team.home_matches),
                joinedload(Team.away_matches)
            ).filter(func.upper(Team.id) == upper_team_id).first()
            
        return self._team_to_dict(team) if team else None

    def get_team_logo(self, team_id: str):
        if not team_id:
            return None
            
        # Try exact match first
        logo = self.db.query(TeamLogo).filter(
            TeamLogo.team_id == team_id
        ).first()
        
        # Fall back to case-insensitive
        if not logo:
            upper_team_id = team_id.upper()
            logo = self.db.query(TeamLogo).filter(
                func.upper(TeamLogo.team_id) == upper_team_id
            ).first()
            
        return self._logo_to_dict(logo) if logo else None
    
    def get_logos_batch(self, team_ids: List[str]) -> Dict[str, bytes]:
        """
        Fetch multiple team logos in a single query
        Returns a dictionary mapping team_id to logo_data
        """
        if not team_ids:
            return {}
        
        logos = self.db.query(TeamLogo).filter(
            TeamLogo.team_id.in_(team_ids)
        ).all()
        
        return {logo.team_id: logo.logo_data for logo in logos}
    
    def get_roster(self, team_id: str, year: str = None):
        if not team_id:
            return []
            
        upper_team_id = team_id.upper()

        # If no year is provided, get the latest season
        if not year:
            latest_season = self.db.query(Season).order_by(Season.name.desc()).first()
            if latest_season:
                year = latest_season.name
            else:
                return []

        # Get season_id for the year
        season = self.db.query(Season).filter(Season.name.ilike(f"%{year}%")).first()
        
        if not season:
            return []
        
        # Use joinedload to prevent N+1 queries
        players = (
            self.db.query(Player)
            .join(PlayerRoster)
            .filter(
                func.upper(PlayerRoster.team_id) == upper_team_id,
                PlayerRoster.season_id == season.id
            )
            .all()
        )
        return [self._player_to_dict(player) for player in players]