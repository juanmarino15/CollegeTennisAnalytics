from sqlalchemy.orm import Session
from sqlalchemy import func
from models import Player, PlayerSeason, PlayerRoster, PlayerWTN, PlayerMatch, PlayerMatchParticipant

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
        return {
            "person_id": wtn.person_id,
            "tennis_id": wtn.tennis_id,
            "season_id": wtn.season_id,
            "wtn_type": wtn.wtn_type,
            "confidence": wtn.confidence,
            "tennis_number": wtn.tennis_number,
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
            "score_string": match.score_string
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

    def get_player_wtn(self, player_id: str):
        if player_id:
            upper_player_id = player_id.upper()
            wtns = self.db.query(PlayerWTN).filter(
                func.upper(PlayerWTN.person_id) == upper_player_id
            ).all()
            return [self._wtn_to_dict(wtn) for wtn in wtns]
        return []

    def get_player_seasons(self, player_id: str):
        if player_id:
            upper_player_id = player_id.upper()
            seasons = self.db.query(PlayerSeason).filter(
                func.upper(PlayerSeason.person_id) == upper_player_id
            ).all()
            return [self._season_to_dict(season) for season in seasons]
        return []

    def get_player_matches(self, player_id: str):
        if player_id:
            upper_player_id = player_id.upper()
            matches = self.db.query(PlayerMatch).join(PlayerMatchParticipant).filter(
                func.upper(PlayerMatchParticipant.person_id) == upper_player_id
            ).all()
            return [self._match_to_dict(match) for match in matches]
        return []