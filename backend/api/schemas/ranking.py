# api/schemas/ranking.py

from typing import Optional, List
from datetime import datetime, date
from pydantic import BaseModel

from api.schemas.base import BaseSchema

# Team rankings schemas
class RankingListBase(BaseSchema):
    id: str
    division_type: str
    gender: str
    match_format: str
    publish_date: Optional[datetime] = None
    planned_publish_date: Optional[date] = None

class RankingListResponse(RankingListBase):
    date_range_start: datetime
    date_range_end: datetime

class TeamRankingBase(BaseSchema):
    rank: int
    points: float
    wins: int
    losses: int
    team_name: str
    conference: Optional[str] = None

class TeamRankingResponse(TeamRankingBase):
    team_id: str
    ranking_list_id: str

class TeamRankingHistoryItem(TeamRankingBase):
    publish_date: datetime
    ranking_list_id: str

class TeamRankingHistory(BaseSchema):
    team_id: str
    team_name: str
    rankings: List[TeamRankingHistoryItem]

# Player rankings schemas
class PlayerRankingListBase(BaseSchema):
    id: str
    division_type: str
    gender: str
    match_format: str  # "SINGLES" or "DOUBLES"
    publish_date: Optional[datetime] = None
    planned_publish_date: Optional[date] = None

class PlayerRankingListResponse(PlayerRankingListBase):
    date_range_start: datetime
    date_range_end: datetime

class PlayerRankingBase(BaseSchema):
    rank: int
    points: float
    wins: int
    losses: int
    player_name: str
    team_name: str
    conference: Optional[str] = None

class PlayerRankingResponse(PlayerRankingBase):
    player_id: str
    team_id: str
    ranking_list_id: str

class PlayerRankingHistoryItem(PlayerRankingBase):
    publish_date: datetime
    ranking_list_id: str

class PlayerRankingHistory(BaseSchema):
    player_id: str
    player_name: str
    rankings: List[PlayerRankingHistoryItem]

class DoublesRankingBase(BaseSchema):
    rank: int
    points: float
    wins: int
    losses: int
    player1_name: str
    player2_name: str
    team_name: str
    conference: Optional[str] = None

class DoublesRankingResponse(DoublesRankingBase):
    team_id: str
    player1_id: str
    player2_id: str
    ranking_list_id: str