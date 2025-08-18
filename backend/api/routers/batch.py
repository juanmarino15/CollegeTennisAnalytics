# api/routers/batch.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any
from api.database import get_db, get_quick_db
from api.cache.memory_cache import cache, cached
import json

router = APIRouter()

@router.post("/batch/teams")
def get_teams_batch(
    team_ids: List[str],
    db: Session = Depends(get_db)
):
    """Get multiple teams in one request with caching"""
    if len(team_ids) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 teams per request")
    
    result = {}
    uncached_ids = []
    
    # Check cache first
    for team_id in team_ids:
        cached_team = cache.get(f"team:{team_id}")
        if cached_team:
            result[team_id] = cached_team
        else:
            uncached_ids.append(team_id)
    
    # Fetch uncached teams in one query
    if uncached_ids:
        # Use raw SQL for efficiency
        query = text("""
            SELECT id, name, abbreviation, division, conference, region, gender
            FROM teams
            WHERE id = ANY(:team_ids)
        """)
        
        teams = db.execute(query, {"team_ids": uncached_ids}).fetchall()
        
        for team in teams:
            team_dict = dict(team)
            result[team.id] = team_dict
            # Cache for 1 hour
            cache.set(f"team:{team.id}", team_dict, ttl=3600)
    
    return result

@router.post("/batch/match-scores")
def get_match_scores_batch(
    match_ids: List[str],
    db: Session = Depends(get_db)
):
    """Get multiple match scores in one request"""
    if len(match_ids) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 matches per request")
    
    result = {}
    uncached_ids = []
    
    # Check cache first
    for match_id in match_ids:
        cached_score = cache.get(f"score:{match_id}")
        if cached_score:
            result[match_id] = cached_score
        else:
            uncached_ids.append(match_id)
    
    if uncached_ids:
        # Get all scores in one query
        query = text("""
            SELECT 
                match_id,
                MAX(CASE WHEN is_home_team = true THEN score ELSE 0 END) as home_score,
                MAX(CASE WHEN is_home_team = false THEN score ELSE 0 END) as away_score,
                MAX(CASE WHEN is_home_team = true THEN did_win ELSE false END) as home_won,
                MAX(CASE WHEN is_home_team = false THEN did_win ELSE false END) as away_won
            FROM match_teams
            WHERE match_id = ANY(:match_ids)
            GROUP BY match_id
        """)
        
        scores = db.execute(query, {"match_ids": uncached_ids}).fetchall()
        
        for score in scores:
            score_dict = {
                "home_team_score": int(score.home_score or 0),
                "away_team_score": int(score.away_score or 0),
                "home_team_won": score.home_won,
                "away_team_won": score.away_won
            }
            result[score.match_id] = score_dict
            # Cache completed match scores for 24 hours
            cache.set(f"score:{score.match_id}", score_dict, ttl=86400)
    
    return result

@router.get("/batch/matches-with-data")
@cached(ttl=300)  # Cache for 5 minutes
def get_matches_with_data(
    date: str,
    db: Session = Depends(get_db)
):
    """Get all matches for a date with teams and scores in one request"""
    
    # Check cache first
    cache_key = f"matches_full:{date}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return cached_result
    
    # Efficient single query to get everything
    query = text("""
        WITH match_scores AS (
            SELECT 
                match_id,
                MAX(CASE WHEN is_home_team = true THEN score ELSE 0 END) as home_score,
                MAX(CASE WHEN is_home_team = false THEN score ELSE 0 END) as away_score
            FROM match_teams
            GROUP BY match_id
        )
        SELECT 
            m.id as match_id,
            m.start_date,
            m.scheduled_time,
            m.is_conference_match,
            m.gender as match_gender,
            m.completed,
            m.home_team_id,
            m.away_team_id,
            ht.name as home_team_name,
            ht.abbreviation as home_team_abbr,
            ht.conference as home_team_conf,
            at.name as away_team_name,
            at.abbreviation as away_team_abbr,
            at.conference as away_team_conf,
            ms.home_score,
            ms.away_score
        FROM matches m
        LEFT JOIN teams ht ON m.home_team_id = ht.id
        LEFT JOIN teams at ON m.away_team_id = at.id
        LEFT JOIN match_scores ms ON m.id = ms.match_id
        WHERE DATE(m.start_date) = :match_date
        ORDER BY m.scheduled_time, m.start_date
    """)
    
    matches = db.execute(query, {"match_date": date}).fetchall()
    
    result = {
        "date": date,
        "matches": [dict(match) for match in matches]
    }
    
    # Cache for 5 minutes
    cache.set(cache_key, result, ttl=300)
    
    return result