class PlayerBase(BaseSchema):
   person_id: str
   tennis_id: str | None
   first_name: str
   last_name: str
   avatar_url: str | None

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
   confidence: int | None
   tennis_number: float | None
   is_ranked: bool

class PlayerCreate(PlayerBase):
   pass

class PlayerResponse(PlayerBase):
   class Config:
       from_attributes = True
