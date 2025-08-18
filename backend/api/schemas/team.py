from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

class TeamBase(BaseModel):
    id: str
    name: str
    abbreviation: Optional[str] = None
    division: Optional[str] = None
    conference: Optional[str] = None
    region: Optional[str] = None
    typename: Optional[str] = None
    gender: Optional[str] = None

class TeamCreate(TeamBase):
    pass

class TeamResponse(TeamBase):
    class Config:
        orm_mode = True

class TeamLogo(BaseModel):
    team_id: str
    logo_data: bytes
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True

class TeamBatchRequest(BaseModel):
    team_ids: List[str]

class TeamLogoBatchResponse(BaseModel):
    """Response for batch logo requests"""
    logos: dict  # Will contain team_id -> base64 encoded logo mapping