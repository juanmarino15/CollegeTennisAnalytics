from typing import Optional
from api.schemas.base import BaseSchema
 
class SchoolBase(BaseSchema):
   id: str
   name: str
   conference: Optional[str] = None
   ita_region: Optional[str] = None
   ranking_award_region: Optional[str] = None
   usta_section: Optional[str] = None
   man_id: Optional[str] = None
   woman_id: Optional[str] = None
   division: Optional[str] = None
   mailing_address: Optional[str] = None
   city: Optional[str] = None
   state: Optional[str] = None
   zip_code: Optional[str] = None
   team_type: Optional[str] = None

class SchoolCreate(SchoolBase):
   pass

class SchoolResponse(SchoolBase):
   pass