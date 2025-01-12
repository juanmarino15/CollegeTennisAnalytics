from typing import Optional
from api.schemas.base import BaseSchema

class PlayerStats(BaseSchema):
   singles_wins: int
   singles_losses: int
   singles_win_pct: float
   doubles_wins: int 
   doubles_losses: int
   doubles_win_pct: float
   wtn_singles: Optional[float] = None
   wtn_doubles: Optional[float] = None

class TeamStats(BaseSchema):
   total_wins: int
   total_losses: int
   conference_wins: int
   conference_losses: int
   home_wins: int
   home_losses: int
   away_wins: int
   away_losses: int