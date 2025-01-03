from sqlalchemy.orm import Session
from sqlalchemy import func
from models import Player, PlayerSeason, PlayerRoster, PlayerWTN, PlayerMatch, PlayerMatchParticipant

class PlayerService:
   def __init__(self, db: Session):
       self.db = db
   
   def get_players(self, team_id: str = None):
       query = self.db.query(Player)
       if team_id:
           upper_team_id = team_id.upper()
           query = query.join(PlayerRoster).filter(
               func.upper(PlayerRoster.team_id) == upper_team_id
           )
       return query.all()

   def get_player(self, player_id: str):
       if player_id:
           upper_player_id = player_id.upper()
           return self.db.query(Player).filter(
               func.upper(Player.person_id) == upper_player_id
           ).first()
       return None

   def get_player_wtn(self, player_id: str):
       if player_id:
           upper_player_id = player_id.upper()
           return self.db.query(PlayerWTN).filter(
               func.upper(PlayerWTN.person_id) == upper_player_id
           ).all()
       return []

   def get_player_seasons(self, player_id: str):
       if player_id:
           upper_player_id = player_id.upper()
           return self.db.query(PlayerSeason).filter(
               func.upper(PlayerSeason.person_id) == upper_player_id
           ).all()
       return []

   def get_player_matches(self, player_id: str):
       if player_id:
           upper_player_id = player_id.upper()
           return self.db.query(PlayerMatch).join(PlayerMatchParticipant).filter(
               func.upper(PlayerMatchParticipant.person_id) == upper_player_id
           ).all()
       return []