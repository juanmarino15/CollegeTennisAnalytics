# api/schemas/base.py
from pydantic import BaseModel, ConfigDict

class BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # New Pydantic v2 style

# api/schemas/match.py
from datetime import datetime
from typing import Optional
from .base import BaseSchema

class MatchBase(BaseSchema):
    id: str
    start_date: datetime
    timezone: Optional[str]  # More explicit about Optional
    no_scheduled_time: bool
    is_conference_match: bool
    gender: str
    home_team_id: Optional[str]  # Make optional since some might be null
    away_team_id: Optional[str]
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    season: str
    completed: bool
    scheduled_time: Optional[datetime]

class MatchLineup(BaseSchema):
    id: str
    match_id: str
    match_type: str
    position: int
    side1_player1_id: str
    side1_player2_id: Optional[str]
    side1_score: str
    side1_won: bool
    side2_player1_id: str
    side2_player2_id: Optional[str]
    side2_score: str
    side2_won: bool

class MatchCreate(MatchBase):
    pass

class MatchResponse(MatchBase):
    pass  # Inherits config from BaseSchema