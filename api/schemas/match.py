# api/schemas/match.py
from datetime import datetime
from pydantic import BaseModel
from .base import BaseSchema

class MatchBase(BaseSchema):
    id: str
    start_date: datetime
    timezone: str | None
    no_scheduled_time: bool
    is_conference_match: bool
    gender: str
    home_team_id: str
    away_team_id: str
    season: str
    completed: bool
    scheduled_time: datetime | None

class MatchLineup(BaseSchema):
    id: str
    match_id: str
    match_type: str
    position: int
    side1_player1_id: str
    side1_player2_id: str | None
    side1_score: str
    side1_won: bool
    side2_player1_id: str
    side2_player2_id: str | None
    side2_score: str
    side2_won: bool

class MatchCreate(MatchBase):
    pass

class MatchResponse(MatchBase):
    class Config:
        from_attributes = True