# src/models.py
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Float, LargeBinary
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime


Base = declarative_base()

class Match(Base):
    __tablename__ = 'matches'
    
    id = Column(String, primary_key=True)
    start_date = Column(DateTime)
    timezone = Column(String)
    no_scheduled_time = Column(Boolean)
    is_conference_match = Column(Boolean)
    gender = Column(String)
    typename = Column(String)  
    home_team_id = Column(String, ForeignKey('teams.id'))
    away_team_id = Column(String, ForeignKey('teams.id'))
    season = Column(String)
    side_numbers = Column(Integer)
    completed = Column(Boolean)
    scheduled_time = Column(DateTime)
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matches")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matches")
    web_links = relationship("WebLink", back_populates="match")
    location = relationship("Location", back_populates="match", uselist=False)

class Team(Base):
    __tablename__ = 'teams'
    
    id = Column(String, primary_key=True)
    name = Column(String)
    abbreviation = Column(String)
    division = Column(String)
    conference = Column(String)
    region = Column(String)
    typename = Column(String)
    gender = Column(String)
    
    # Relationships
    home_matches = relationship("Match", foreign_keys=[Match.home_team_id], back_populates="home_team")
    away_matches = relationship("Match", foreign_keys=[Match.away_team_id], back_populates="away_team")

class MatchTeam(Base):
    __tablename__ = 'match_teams'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String, ForeignKey('matches.id'))
    team_id = Column(String, ForeignKey('teams.id'))
    score = Column(Float)
    did_win = Column(Boolean)
    side_number = Column(Integer)
    is_home_team = Column(Boolean)
    order_of_play = Column(Integer)
    team_position = Column(String)  # 'home' or 'away'

class WebLink(Base):
    __tablename__ = 'web_links'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String, ForeignKey('matches.id'))
    name = Column(String)
    url = Column(String)
    typename = Column(String)
    
    match = relationship("Match", back_populates="web_links")

class Location(Base):
    __tablename__ = 'locations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timezone = Column(String)
    timezone_offset = Column(String)
    region = Column(String)
    match_id = Column(String, ForeignKey('matches.id'), unique=True)
    
    match = relationship("Match", back_populates="location")

class TeamLogo(Base):
    __tablename__ = 'team_logos'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(String, ForeignKey('teams.id'), unique=True)
    logo_data = Column(LargeBinary)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship with Team
    team = relationship("Team", backref="logo")

class SchoolInfo(Base):
    __tablename__ = 'school_info'
    
    id = Column(String, primary_key=True)  # This will be the school_id we get from scraping
    name = Column(String)
    conference = Column(String)
    ita_region = Column(String)
    ranking_award_region = Column(String)
    usta_section = Column(String)
    man_id = Column(String)
    woman_id = Column(String)
    division = Column(String)
    mailing_address = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(String)
    team_type = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)