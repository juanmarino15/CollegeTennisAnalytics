# api/routers/seasons.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from api.database import get_db
from models import Season as SeasonModel
from typing import Optional

router = APIRouter()

# Define a simple Pydantic model for Season
from pydantic import BaseModel, ConfigDict

class Season(BaseModel):
    id: str
    name: str
    status: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)

@router.get("/", response_model=List[Season])
def get_seasons(db: Session = Depends(get_db)):
    """Get all seasons"""
    seasons = db.query(SeasonModel).all()
    # Convert SQLAlchemy models to dictionaries first
    seasons_dict = [
        {
            "id": season.id,
            "name": season.name,
            "status": season.status,
            "start_date": str(season.start_date) if season.start_date else None,
            "end_date": str(season.end_date) if season.end_date else None
        }
        for season in seasons
    ]
    # Return the list of dictionaries which FastAPI will convert to Season models
    return seasons_dict

@router.get("/{season_id}", response_model=Season)
def get_season(season_id: str, db: Session = Depends(get_db)):
    """Get a single season by ID"""
    season = db.query(SeasonModel).filter(SeasonModel.id == season_id).first()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")
    return {
        "id": season.id,
        "name": season.name,
        "status": season.status,
        "start_date": str(season.start_date) if season.start_date else None,
        "end_date": str(season.end_date) if season.end_date else None
    }

@router.get("/by-name/{name}", response_model=Season)
def get_season_by_name(name: str, db: Session = Depends(get_db)):
    """Get a single season by name"""
    season = db.query(SeasonModel).filter(SeasonModel.name == name).first()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")
    return {
        "id": season.id,
        "name": season.name,
        "status": season.status,
        "start_date": str(season.start_date) if season.start_date else None,
        "end_date": str(season.end_date) if season.end_date else None
    }