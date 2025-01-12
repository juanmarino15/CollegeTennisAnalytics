from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Date 
from models import Match, MatchLineup, MatchTeam
from datetime import date

class MatchService:
    def __init__(self, db: Session):
        self.db = db

    def _match_to_dict(self, match):
        """Convert Match model to dictionary"""
        return {
            "id": match.id,
            "start_date": match.start_date,
            "timezone": match.timezone,
            "no_scheduled_time": match.no_scheduled_time,
            "is_conference_match": match.is_conference_match,
            "gender": match.gender,
            "home_team_id": match.home_team_id,
            "away_team_id": match.away_team_id,
            "season": match.season,
            "completed": match.completed,
            "scheduled_time": match.scheduled_time
        }

    def _lineup_to_dict(self, lineup):
        """Convert MatchLineup model to dictionary"""
        return {
            "id": lineup.id,
            "match_id": lineup.match_id,
            "match_type": lineup.match_type,
            "position": lineup.position,
            "side1_player1_id": lineup.side1_player1_id,
            "side1_player2_id": lineup.side1_player2_id,
            "side1_score": lineup.side1_score,
            "side1_won": lineup.side1_won,
            "side2_player1_id": lineup.side2_player1_id,
            "side2_player2_id": lineup.side2_player2_id,
            "side2_score": lineup.side2_score,
            "side2_won": lineup.side2_won
        }

    from sqlalchemy import func, cast, Date

    def get_matches(self, date: date = None, team_id: str = None):
        query = self.db.query(Match)
        if date:
            # Cast the datetime to date for comparison
            query = query.filter(cast(Match.start_date, Date) == date)
        if team_id:
            upper_team_id = team_id.upper() if team_id else None
            query = query.filter(
                (func.upper(Match.home_team_id) == upper_team_id) | 
                (func.upper(Match.away_team_id) == upper_team_id)
            )
        matches = query.all()
        return [self._match_to_dict(match) for match in matches]

    def get_match(self, match_id: str):
        if match_id:
            upper_match_id = match_id.upper()
            match = self.db.query(Match).filter(
                func.upper(Match.id) == upper_match_id
            ).first()
            return self._match_to_dict(match) if match else None
        return None

    def get_match_lineup(self, match_id: str):
        if match_id:
            upper_match_id = match_id.upper()
            lineups = self.db.query(MatchLineup).filter(
                func.upper(MatchLineup.match_id) == upper_match_id
            ).all()
            return [self._lineup_to_dict(lineup) for lineup in lineups]
        return []