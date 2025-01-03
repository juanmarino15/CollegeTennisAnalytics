from sqlalchemy.orm import Session
from sqlalchemy import func
from models import Match, MatchLineup, MatchTeam
from datetime import date

class MatchService:
    def __init__(self, db: Session):
        self.db = db

    def get_matches(self, date: date = None, team_id: str = None):
        query = self.db.query(Match)
        if date:
            query = query.filter(Match.start_date == date)
        if team_id:
            upper_team_id = team_id.upper() if team_id else None
            query = query.filter(
                (func.upper(Match.home_team_id) == upper_team_id) | 
                (func.upper(Match.away_team_id) == upper_team_id)
            )
        return query.all()

    def get_match(self, match_id: str):
        if match_id:
            upper_match_id = match_id.upper()
            return self.db.query(Match).filter(
                func.upper(Match.id) == upper_match_id
            ).first()
        return None

    def get_match_lineup(self, match_id: str):
        if match_id:
            upper_match_id = match_id.upper()
            return self.db.query(MatchLineup).filter(
                func.upper(MatchLineup.match_id) == upper_match_id
            ).all()
        return []