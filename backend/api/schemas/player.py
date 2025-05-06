from typing import Optional
from api.schemas.base import BaseSchema
from typing import Optional, List
from datetime import datetime

class PlayerBase(BaseSchema):
   person_id: str
   tennis_id: Optional[str] = None
   first_name: str
   last_name: str
   avatar_url: Optional[str] = None

class PlayerSeason(BaseSchema):
   person_id: str 
   tennis_id: str
   season_id: str
   class_year: str

class PlayerWTN(BaseSchema):
   person_id: str
   tennis_id: str
   season_id: str
   wtn_type: str
   confidence: Optional[int] = None
   tennis_number: Optional[float] = None
   is_ranked: bool

class PlayerCreate(PlayerBase):
   pass

class PlayerResponse(PlayerBase):
   class Config:
       from_attributes = True

# Schema for player's team information
class PlayerTeamInfo(BaseSchema):
    team_id: str
    team_name: str
    abbreviation: Optional[str] = None
    conference: Optional[str] = None
    gender: Optional[str] = None
    season_id: Optional[str] = None

# Schema for player statistics
class PlayerStatsInfo(BaseSchema):
    singles_wins: int
    singles_losses: int
    singles_win_pct: float
    doubles_wins: int
    doubles_losses: int
    doubles_win_pct: float
    wtn_singles: Optional[float] = None
    wtn_doubles: Optional[float] = None

# Schema for position data
class PositionData(BaseSchema):
    position: int
    matches_count: int
    wins: int
    losses: int

# Schema for player positions
class PlayerPositions(BaseSchema):
    singles: List[PositionData]
    doubles: List[PositionData]

# Additional schema for enhanced match results (if needed)
class PlayerMatchResult(BaseSchema):
    id: str
    match_id: str
    date: datetime
    opponent_name: str
    opponent_team_id: Optional[str] = None
    is_home: bool
    match_type: str
    position: int
    score: str
    won: bool
    partner_name: Optional[str] = None
    opponent_name1: str
    opponent_name2: Optional[str] = None

class PlayerSearchResult(BaseSchema):
    person_id: str
    tennis_id: Optional[str] = None
    first_name: str
    last_name: str
    avatar_url: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    gender: Optional[str] = None
    conference: Optional[str] = None
    division: Optional[str] = None
    season_name: Optional[str] = None
    season_id: Optional[str] = None
    school_name: Optional[str] = None
    school_id: Optional[str] = None
    wtn_singles: Optional[float] = None
    wtn_doubles: Optional[float] = None