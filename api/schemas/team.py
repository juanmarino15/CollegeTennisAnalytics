from .base import BaseSchema
from datetime import datetime

class TeamBase(BaseSchema):
   id: str
   name: str
   abbreviation: str | None
   division: str | None
   conference: str | None
   region: str | None
   typename: str | None
   gender: str | None

class TeamLogo(BaseSchema):
   team_id: str
   logo_data: bytes
   created_at: datetime
   updated_at: datetime

class TeamCreate(TeamBase):
   pass

class TeamResponse(TeamBase):
   pass