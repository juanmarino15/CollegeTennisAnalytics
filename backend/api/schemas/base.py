from typing import Optional
from pydantic import BaseModel
from datetime import datetime

class BaseSchema(BaseModel):
    class Config:
        from_attributes = True

# api/schemas/team.py
class TeamBase(BaseSchema):
    id: str
    name: str
    abbreviation: Optional[str] = None
    division: Optional[str] = None
    conference: Optional[str] = None
    region: Optional[str] = None
    gender: Optional[str] = None

class TeamWithLogo(TeamBase):
    logo_data: Optional[bytes] = None

# api/schemas/player.py
class PlayerBase(BaseSchema):
    person_id: str
    tennis_id: Optional[str] = None
    first_name: str
    last_name: str
    avatar_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime

class PlayerStats(BaseSchema):
    singles_wins: int
    singles_losses: int
    doubles_wins: int
    doubles_losses: int
    wtn_rating: Optional[float] = None

# api/schemas/match.py
class MatchBase(BaseSchema):
    id: str
    start_date: datetime
    home_team_id: str
    away_team_id: str
    completed: bool
    score: Optional[str] = None

class MatchLineupBase(BaseSchema):
    id: str
    match_type: str
    side1_score: str
    side2_score: str
    side1_won: bool
    side2_won: bool