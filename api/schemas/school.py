class SchoolBase(BaseSchema):
   id: str
   name: str
   conference: str | None
   ita_region: str | None
   ranking_award_region: str | None
   usta_section: str | None
   man_id: str | None
   woman_id: str | None
   division: str | None
   mailing_address: str | None
   city: str | None
   state: str | None
   zip_code: str | None
   team_type: str | None

class SchoolCreate(SchoolBase):
   pass

class SchoolResponse(SchoolBase):
   pass