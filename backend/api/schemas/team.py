from typing import Optional
from .base import BaseSchema
from datetime import datetime

class TeamBase(BaseSchema):
   id: str
   name: str
   abbreviation: Optional[str] = None
   division: Optional[str] = None
   conference: Optional[str] = None
   region: Optional[str] = None
   typename: Optional[str] = None
   gender: Optional[str] = None

class TeamLogo(BaseSchema):
   team_id: str
   logo_data: bytes
   created_at: datetime
   updated_at: datetime

class TeamCreate(TeamBase):
   pass

class TeamResponse(TeamBase):
   pass