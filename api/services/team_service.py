from sqlalchemy.orm import Session
from sqlalchemy import func
from models import Team, TeamLogo, PlayerRoster, Player

class TeamService:
   def __init__(self, db: Session):
       self.db = db

   def get_teams(self, conference: str = None):
       query = self.db.query(Team)
       if conference:
           query = query.filter(func.upper(Team.conference) == conference.upper())
       return query.all()

   def get_team(self, team_id: str):
       if team_id:
           upper_team_id = team_id.upper()
           return self.db.query(Team).filter(
               func.upper(Team.id) == upper_team_id
           ).first()
       return None

   def get_team_logo(self, team_id: str):
       if team_id:
           upper_team_id = team_id.upper()
           return self.db.query(TeamLogo).filter(
               func.upper(TeamLogo.team_id) == upper_team_id
           ).first()
       return None

   def get_roster(self, team_id: str):
       if team_id:
           upper_team_id = team_id.upper()
           return self.db.query(Player).join(PlayerRoster).filter(
               func.upper(PlayerRoster.team_id) == upper_team_id
           ).all()
       return []