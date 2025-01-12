# api/routers/schools.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.school_service import SchoolService
from api.schemas.school import SchoolResponse
from api.database import get_db

router = APIRouter()

@router.get("/", response_model=List[SchoolResponse])
def get_schools(
    conference: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = SchoolService(db)
    return service.get_schools(conference=conference)

@router.get("/{school_id}", response_model=SchoolResponse)
def get_school(school_id: str, db: Session = Depends(get_db)):
    service = SchoolService(db)
    school = service.get_school(school_id)
    if not school:
        raise HTTPException(status_code=404, detail="School not found")
    return school

@router.get("/{school_id}/teams")
def get_school_teams(school_id: str, db: Session = Depends(get_db)):
    service = SchoolService(db)
    teams = service.get_school_teams(school_id)
    if not teams:
        return []
    return teams