# api/schemas/tournament.py
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from api.schemas.base import BaseSchema

class EventDivision(BaseSchema):
    gender: Optional[str] = None
    eventType: Optional[str] = None  # "singles" or "doubles"

class TournamentEvent(BaseSchema):
    id: str  # This is the event_id
    division: EventDivision

class TournamentLevel(BaseSchema):
    id: Optional[str] = None
    name: Optional[str] = None
    branding: Optional[str] = None

class TournamentLevelCategory(BaseSchema):
    name: str

class TournamentLocation(BaseSchema):
    id: Optional[str] = None
    name: Optional[str] = None
    geo: Optional[Dict[str, float]] = None

class TournamentOrganization(BaseSchema):
    id: Optional[str] = None
    name: Optional[str] = None
    conference: Optional[str] = None
    division: Optional[str] = None
    urlSegment: Optional[str] = None

class TournamentPrimaryLocation(BaseSchema):
    address1: Optional[str] = None
    address2: Optional[str] = None
    address3: Optional[str] = None
    town: Optional[str] = None
    county: Optional[str] = None
    postcode: Optional[str] = None

class TournamentRegistrationRestrictions(BaseSchema):
    entriesOpenDateTime: Optional[str] = None
    entriesCloseDateTime: Optional[str] = None
    secondsUntilEntriesClose: Optional[float] = None
    secondsUntilEntriesOpen: Optional[float] = None
    timeZone: Optional[str] = None

class TournamentItem(BaseSchema):
    id: str
    identificationCode: Optional[str] = None
    name: Optional[str] = None
    image: Optional[str] = None
    startDateTime: Optional[str] = None
    endDateTime: Optional[str] = None
    timeZone: Optional[str] = None
    isCancelled: Optional[bool] = False
    url: Optional[str] = None
    events: List[TournamentEvent] = []
    level: Optional[TournamentLevel] = None
    levelCategories: List[TournamentLevelCategory] = []
    location: Optional[TournamentLocation] = None
    organization: Optional[TournamentOrganization] = None
    primaryLocation: Optional[TournamentPrimaryLocation] = None
    registrationRestrictions: Optional[TournamentRegistrationRestrictions] = None
    
    # Custom fields to identify type
    _isDualMatch: Optional[bool] = None
    _matchType: Optional[str] = None

class TournamentSearchResult(BaseSchema):
    distance: float = 0
    item: TournamentItem

class TournamentSearchResponse(BaseSchema):
    total: int
    searchResults: List[TournamentSearchResult]

class TournamentStatsResponse(BaseSchema):
    dual_matches: int
    tournaments: int
    tournament_singles_events: int
    tournament_doubles_events: int
    total: int

# Request schemas for filtering
class DateRangeFilter(BaseSchema):
    minDate: Optional[str] = None
    maxDate: Optional[str] = None

class SearchFilter(BaseSchema):
    key: str
    operator: str = "Or"
    items: List[DateRangeFilter]

class SearchOptions(BaseSchema):
    size: int = 25
    from_: int = 0  # 'from' is a reserved keyword, so use 'from_'
    sortKey: str = "date"
    latitude: float = 0
    longitude: float = 0

class TournamentSearchRequest(BaseSchema):
    filters: List[SearchFilter] = []
    options: SearchOptions = SearchOptions()

# Simplified event schemas for database operations
class TournamentEventDB(BaseSchema):
    """Schema for tournament events in database"""
    event_id: str
    tournament_id: str
    gender: Optional[str] = None
    event_type: Optional[str] = None

class TournamentEventCreate(BaseSchema):
    """Schema for creating tournament events"""
    gender: str
    event_type: str

class TournamentEventResponse(BaseSchema):
    """Schema for tournament event responses"""
    event_id: str
    tournament_id: str
    gender: Optional[str] = None
    event_type: Optional[str] = None
    created_at: datetime
    updated_at: datetime

# Enhanced response schemas with event details
class TournamentWithEvents(BaseSchema):
    """Tournament with its events"""
    tournament_id: str
    name: Optional[str] = None
    start_date_time: Optional[datetime] = None
    end_date_time: Optional[datetime] = None
    location_name: Optional[str] = None
    organization_name: Optional[str] = None
    events: List[TournamentEventResponse] = []

class TournamentEventStats(BaseSchema):
    """Statistics for tournament events"""
    total_tournaments: int
    total_events: int
    events_by_gender: Dict[str, int] = {}
    events_by_type: Dict[str, int] = {}
    tournaments_with_both_genders: int
    tournaments_with_both_types: int

# Bulk operation schemas
class BulkTournamentEventUpdate(BaseSchema):
    event_ids: List[str]
    updates: Dict[str, Any]

class BulkTournamentEventResponse(BaseSchema):
    updated_count: int
    failed_updates: List[Dict[str, str]]
    success: bool