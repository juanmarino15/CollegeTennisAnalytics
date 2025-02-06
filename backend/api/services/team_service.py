from sqlalchemy.orm import Session
from sqlalchemy import func
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
        if team_id:
            upper_team_id = team_id.upper()
            team = self.db.query(Team).filter(
                func.upper(Team.id) == upper_team_id
            ).first()
            return self._team_to_dict(team) if team else None
        return None

    def get_team_logo(self, team_id: str):
        if team_id:
            upper_team_id = team_id.upper()
            logo = self.db.query(TeamLogo).filter(
                func.upper(TeamLogo.team_id) == upper_team_id
            ).first()
            return self._logo_to_dict(logo) if logo else None
        return None
    
    def get_roster(self, team_id: str, year: str = None):
        if team_id:
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
        return []
