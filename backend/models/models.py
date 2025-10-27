# models/models.py
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Float, LargeBinary, ForeignKeyConstraint,Date,JSON
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime,date
from uuid import uuid4


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
    teams = relationship("MatchTeam", back_populates="match")


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
    
    match_id = Column(String, ForeignKey('matches.id'), primary_key=True)
    team_id = Column(String, ForeignKey('teams.id'), primary_key=True)
    score = Column(Float)
    did_win = Column(Boolean)
    side_number = Column(Integer)
    is_home_team = Column(Boolean)
    order_of_play = Column(Integer)
    team_position = Column(String)  # 'home' or 'away'

    match = relationship("Match", back_populates="teams")


class WebLink(Base):
    __tablename__ = 'web_links'
    
    match_id = Column(String, ForeignKey('matches.id'), primary_key=True)
    url = Column(String, primary_key=True)
    name = Column(String)
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

class Season(Base):
    __tablename__ = 'seasons'
    
    id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Player(Base):
    __tablename__ = 'players'
    
    person_id = Column(String, primary_key=True)  # personId from API
    tennis_id = Column(String, unique=True)  # tennisId from API
    first_name = Column(String)  # standardGivenName from API
    last_name = Column(String)  # standardFamilyName from API
    avatar_url = Column(String)  # avatarUrl from API
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    def __repr__(self):
        return (f"<Player(person_id={self.person_id}, tennis_id={self.tennis_id}, "
                f"first_name={self.first_name}, last_name={self.last_name}, "
                f"avatar_url={self.avatar_url})>")

class PlayerSeason(Base):
    __tablename__ = 'player_seasons'
    
    person_id = Column(String, ForeignKey('players.person_id'), primary_key=True)
    tennis_id = Column(String)
    season_id = Column(String, ForeignKey('seasons.id'), primary_key=True)
    class_year = Column(String)  # class from API
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PlayerRoster(Base):
    __tablename__ = 'player_rosters'
    
    person_id = Column(String, ForeignKey('players.person_id'), primary_key=True)
    tennis_id = Column(String)
    season_id = Column(String, ForeignKey('seasons.id'), primary_key=True)
    team_id = Column(String,primary_key=True)  
    school_id = Column(String, ForeignKey('school_info.id'))
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PlayerWTN(Base):
    __tablename__ = 'player_wtns'
    
    person_id = Column(String, ForeignKey('players.person_id'), primary_key=True)
    tennis_id = Column(String)
    season_id = Column(String, ForeignKey('seasons.id'), primary_key=True)
    wtn_type = Column(String, primary_key=True)  # type from worldTennisNumbers
    confidence = Column(Integer)  # confidence from worldTennisNumbers
    tennis_number = Column(Float)  # tennisNumber from worldTennisNumbers
    is_ranked = Column(Boolean)  # isRanked from worldTennisNumbers
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PlayerMatch(Base):
    __tablename__ = 'player_matches'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_identifier = Column(String, unique=True) 
    winning_side = Column(String)  # "SIDE1" or "SIDE2"
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    match_type = Column(String)  # "SINGLES", "DOUBLES"
    match_format = Column(String)  # e.g., "SET3-S:6/TB7"
    status = Column(String)  # "COMPLETED", "RETIRED", etc.
    round_name = Column(String)  # e.g., "R256", "R128"
    tournament_id = Column(String)  # providerTournamentId
    score_string = Column(String)  # e.g., "6-1 6-0"
    collection_position = Column(Integer, nullable=True)
    draw_id = Column(String, nullable=True)  
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PlayerMatchSet(Base):
    __tablename__ = 'player_match_sets'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey('player_matches.id'))
    set_number = Column(Integer)  # 1, 2, 3
    winner_games_won = Column(Integer)
    loser_games_won = Column(Integer)
    win_ratio = Column(Float)
    tiebreak_winner_points = Column(Integer, nullable=True)
    tiebreak_loser_points = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    match = relationship("PlayerMatch", backref="sets")

class PlayerMatchParticipant(Base):
    __tablename__ = 'player_match_participants'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey('player_matches.id'))
    person_id = Column(String)  # externalID
    team_id = Column(String)  # from extensions
    side_number = Column(String)  # "SIDE1" or "SIDE2"
    family_name = Column(String)  # nativeFamilyName
    given_name = Column(String)  # nativeGivenName
    is_winner = Column(Boolean)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    match = relationship("PlayerMatch", backref="participants")


class MatchLineup(Base):
    __tablename__ = 'match_lineups'
    
    id = Column(String, primary_key=True)  # Using the tie_match id from the API
    match_id = Column(String, ForeignKey('matches.id'))
    match_type = Column(String)  # 'SINGLES' or 'DOUBLES'
    position = Column(Integer)  # collectionPosition
    collection_id = Column(String)
    
    # Side 1 (usually home team)
    side1_player1_id = Column(String, ForeignKey('players.person_id'))
    side1_player2_id = Column(String, ForeignKey('players.person_id'), nullable=True)  # For doubles
    side1_score = Column(String)
    side1_won = Column(Boolean)
    
    # Side 2 (usually away team)
    side2_player1_id = Column(String, ForeignKey('players.person_id'))
    side2_player2_id = Column(String, ForeignKey('players.person_id'), nullable=True)  # For doubles
    side2_score = Column(String)
    side2_won = Column(Boolean)

    side1_name = Column(String, nullable=True)
    side2_name = Column(String, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    match = relationship("Match")
    side1_player1 = relationship("Player", foreign_keys=[side1_player1_id])
    side1_player2 = relationship("Player", foreign_keys=[side1_player2_id])
    side2_player1 = relationship("Player", foreign_keys=[side2_player1_id])
    side2_player2 = relationship("Player", foreign_keys=[side2_player2_id])

class MatchLineupSet(Base):
    __tablename__ = 'match_lineup_sets'
    
    id = Column(Integer, primary_key=True, autoincrement=True)  # Keep auto-incrementing for sets
    lineup_id = Column(String, ForeignKey('match_lineups.id'))
    set_number = Column(Integer)
    side1_score = Column(Integer)
    side2_score = Column(Integer)
    side1_tiebreak = Column(Integer, nullable=True)
    side2_tiebreak = Column(Integer, nullable=True)
    side1_won = Column(Boolean)
    
    lineup = relationship("MatchLineup", backref="sets")


####RANKINGS#########
# Rankings models
class RankingList(Base):
    __tablename__ = 'ranking_lists'
    
    id = Column(String, primary_key=True)
    publish_date = Column(DateTime)
    planned_publish_date = Column(Date)
    division_type = Column(String)
    gender = Column(String)
    match_format = Column(String)
    date_range_start = Column(DateTime)
    date_range_end = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship defined later

class TeamRanking(Base):
    __tablename__ = 'team_rankings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ranking_list_id = Column(String, ForeignKey('ranking_lists.id'))
    team_id = Column(String, ForeignKey('teams.id'))
    rank = Column(Integer)
    points = Column(Float)
    wins = Column(Integer)
    losses = Column(Integer)
    team_name = Column(String)
    conference = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    ranking_list = relationship("RankingList", back_populates="team_rankings")
    team = relationship("Team", back_populates="rankings")

# Add relationships to existing models
RankingList.team_rankings = relationship("TeamRanking", back_populates="ranking_list")
Team.rankings = relationship("TeamRanking", back_populates="team")

class PlayerRankingList(Base):
    __tablename__ = 'player_ranking_lists'
    
    id = Column(String, primary_key=True)  # e.g., "2024-25_d1_men_singles_p13_v1"
    publish_date = Column(DateTime)
    planned_publish_date = Column(Date)
    division_type = Column(String)  # e.g., "DIV1"
    gender = Column(String)  # "M" or "F"
    match_format = Column(String)  # "SINGLES" or "DOUBLES"
    date_range_start = Column(DateTime)
    date_range_end = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship defined below
    player_rankings = relationship("PlayerRanking", back_populates="ranking_list")

class PlayerRanking(Base):
    __tablename__ = 'player_rankings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ranking_list_id = Column(String, ForeignKey('player_ranking_lists.id'))
    player_id = Column(String, ForeignKey('players.person_id'))
    team_id = Column(String, ForeignKey('teams.id'))
    rank = Column(Integer)
    points = Column(Float)
    wins = Column(Integer)
    losses = Column(Integer)
    player_name = Column(String)
    team_name = Column(String)
    conference = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    ranking_list = relationship("PlayerRankingList", back_populates="player_rankings")
    player = relationship("Player", back_populates="rankings")
    team = relationship("Team", back_populates="player_rankings")

Player.rankings = relationship("PlayerRanking", back_populates="player")
Team.player_rankings = relationship("PlayerRanking", back_populates="team")

class DoublesRanking(Base):
    __tablename__ = 'doubles_rankings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ranking_list_id = Column(String, ForeignKey('player_ranking_lists.id'))
    team_id = Column(String, ForeignKey('teams.id'))
    player1_id = Column(String, ForeignKey('players.person_id'))
    player2_id = Column(String, ForeignKey('players.person_id'))
    rank = Column(Integer)
    points = Column(Float)
    wins = Column(Integer)
    losses = Column(Integer)
    player1_name = Column(String)
    player2_name = Column(String)
    team_name = Column(String)
    conference = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    ranking_list = relationship("PlayerRankingList")
    team = relationship("Team")
    player1 = relationship("Player", foreign_keys=[player1_id])
    player2 = relationship("Player", foreign_keys=[player2_id])

class PlayerSearchView(Base):
    __tablename__ = 'player_search_view'
    
    person_id = Column(String, primary_key=True)
    tennis_id = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    avatar_url = Column(String)
    team_id = Column(String)
    team_name = Column(String)
    gender = Column(String)
    conference = Column(String)
    division = Column(String)
    season_name = Column(String)
    season_id = Column(String)
    school_name = Column(String)
    school_id = Column(String)
    wtn_singles = Column(Float)
    wtn_doubles = Column(Float)


####Tournaments#########

class Tournament(Base):
    __tablename__ = 'tournaments'
    
    tournament_id = Column(String, primary_key=True)  # Tournament ID
    identification_code = Column(String)  # e.g., "25-79395"
    name = Column(String)  # e.g., "CCIW Fall Invite"
    image = Column(String, nullable=True)
    is_cancelled = Column(Boolean, default=False)
    cancelled_at = Column(DateTime, nullable=True)
    start_date_time = Column(DateTime)
    end_date_time = Column(DateTime)
    time_zone = Column(String)
    time_zone_start_date_time = Column(DateTime)
    time_zone_end_date_time = Column(DateTime)
    url = Column(String, nullable=True)
    root_provider_id = Column(String)
    
    # Location information
    location_id = Column(String)
    location_name = Column(String)  # e.g., "Carthage College"
    primary_location_town = Column(String)  # e.g., "Kenosha"
    primary_location_county = Column(String)  # e.g., "WI"
    primary_location_address1 = Column(String, nullable=True)
    primary_location_address2 = Column(String, nullable=True)
    primary_location_address3 = Column(String, nullable=True)
    primary_location_postcode = Column(String, nullable=True)
    geo_latitude = Column(Float, default=0)
    geo_longitude = Column(Float, default=0)
    
    # Level information
    level_id = Column(String)
    level_name = Column(String)  # e.g., "DII, DIII, NAIA & JUCO (33-64 players)"
    level_branding = Column(String, nullable=True)
    
    # Organization information
    organization_id = Column(String)
    organization_name = Column(String)  # e.g., "Carthage College (M)"
    organization_conference = Column(String)  # e.g., "College Conference of Illinois and Wisconsin"
    organization_division = Column(String)  # e.g., "DIV_III"
    organization_url_segment = Column(String)
    organization_parent_region_id = Column(String, nullable=True)
    organization_region_id = Column(String, nullable=True)
    
    # Registration information
    entries_open_date_time = Column(DateTime, nullable=True)
    entries_close_date_time = Column(DateTime, nullable=True)
    seconds_until_entries_close = Column(Float, nullable=True)
    seconds_until_entries_open = Column(Float, nullable=True)
    registration_time_zone = Column(String, nullable=True)
    
    # Classification
    is_dual_match = Column(Boolean, default=False)  # True for team vs team matches, False for tournaments
    tournament_type = Column(String)  # 'DUAL_MATCH' or 'TOURNAMENT'

    gender = Column(String)
    event_types = Column(String)
    level_category = Column(String) 
    registration_status = Column(String)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TournamentEvent(Base):
    __tablename__ = 'tournament_events'
    
    event_id = Column(String, primary_key=True)  # Event ID from API
    tournament_id = Column(String, ForeignKey('tournaments.tournament_id'))
    gender = Column(String)  # 'boys', 'girls', 'mixed'
    event_type = Column(String)  # 'singles', 'doubles'
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    tournament = relationship("Tournament", back_populates="events")

Tournament.events = relationship("TournamentEvent", back_populates="tournament")

class TournamentPlayer(Base):
    __tablename__ = 'tournament_players'
    
    id = Column(String, primary_key=True)  # We'll create a unique ID from tournament_id + player_id
    tournament_id = Column(String, ForeignKey('tournaments.tournament_id'))
    
    # Player information
    player_id = Column(String)  # From playerId.value or playerCustomIds where key='personId'
    first_name = Column(String)  # playerFirstName
    last_name = Column(String)  # playerLastName
    player_name = Column(String)  # playerName (full name)
    gender = Column(String)  # playerGender
    city = Column(String, nullable=True)  # playerCity
    state = Column(String, nullable=True)  # playerState
    
    # Registration details from API
    registration_date = Column(DateTime, nullable=True)  # registrationDate
    selection_status = Column(String, nullable=True)  # selectionStatus ("SELECTED", etc.)
    selection_index = Column(Integer, nullable=True)  # selectionIndex
    
    # Event participation - store which events they're in
    events_participating = Column(String)  # Comma-separated: "singles", "doubles", "singles,doubles"
    singles_event_id = Column(String, nullable=True)  # Singles event ID if participating
    doubles_event_id = Column(String, nullable=True)  # Doubles event ID if participating
    
    # Player 2 information (for doubles - current player is player 1)
    player2_id = Column(String, nullable=True)  # Player 2's player ID
    player2_first_name = Column(String, nullable=True)
    player2_last_name = Column(String, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    tournament = relationship("Tournament", back_populates="players")

Tournament.players = relationship("TournamentPlayer", back_populates="tournament")

class TournamentDraw(Base):
    __tablename__ = 'tournament_draws'
    
    draw_id = Column(String, primary_key=True)  # drawId from API
    tournament_id = Column(String, ForeignKey('tournaments.tournament_id'))
    event_id = Column(String)  # eventId
    
    # Draw information for visualization
    draw_name = Column(String)  # "Men's Singles", "Women's Doubles"
    draw_type = Column(String)  # "SINGLE_ELIMINATION", "ROUND_ROBIN"
    draw_size = Column(Integer)  # Number of players/teams in draw
    event_type = Column(String)  # "SINGLES", "DOUBLES"
    gender = Column(String)  # "MALE", "FEMALE", "MIXED"
    
    # Draw status
    draw_completed = Column(Boolean, default=False)
    draw_active = Column(Boolean, default=False)
    
    # Match format
    match_up_format = Column(String)  # e.g., "SET3-S:6/TB7"
    
    # Timestamps from API
    updated_at_api = Column(DateTime)  # updatedAt from API
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    tournament = relationship("Tournament", back_populates="draws")
    matches = relationship("TournamentMatch", back_populates="draw")

class TournamentMatch(Base):
    __tablename__ = 'tournament_matches'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_up_id = Column(String, unique=True, nullable=False)  # matchUpId from API
    draw_id = Column(String, ForeignKey('tournament_draws.draw_id'))
    tournament_id = Column(String, ForeignKey('tournaments.tournament_id'))
    event_id = Column(String)  # eventId
    
    # Match structure information
    round_name = Column(String)  # e.g., "Round 1", "Quarterfinals"
    round_number = Column(Integer)  # numeric round
    round_position = Column(Integer)  # position within round
    match_type = Column(String)  # match type
    match_format = Column(String)  # e.g., "SET3-S:6/TB7"
    match_status = Column(String)  # "COMPLETED", "SCHEDULED", etc.
    stage = Column(String)  # tournament stage
    structure_name = Column(String)  # structure name
    
    # Side 1 complete data
    side1_participant_id = Column(String)
    side1_participant_name = Column(String)
    side1_draw_position = Column(Integer)
    side1_seed_number = Column(Integer)
    side1_school_name = Column(String)
    side1_school_id = Column(String)
    side1_player1_id = Column(String)
    side1_player1_name = Column(String)
    side1_player2_id = Column(String)  # For doubles
    side1_player2_name = Column(String)  # For doubles
    
    # Side 2 complete data
    side2_participant_id = Column(String)
    side2_participant_name = Column(String)
    side2_draw_position = Column(Integer)
    side2_seed_number = Column(Integer)
    side2_school_name = Column(String)
    side2_school_id = Column(String)
    side2_player1_id = Column(String)
    side2_player1_name = Column(String)
    side2_player2_id = Column(String)  # For doubles
    side2_player2_name = Column(String)  # For doubles
    
    # Match outcome
    winning_side = Column(Integer)  # 1 or 2
    winner_participant_id = Column(String)
    winner_participant_name = Column(String)
    
    # Scores
    score_side1 = Column(String)  # e.g., "6-1 6-0"
    score_side2 = Column(String)  # e.g., "1-6 0-6"
    
    # Scheduling
    scheduled_date = Column(Date)
    scheduled_time = Column(DateTime)
    venue_name = Column(String)
    
    # API timestamps
    created_at_api = Column(DateTime)
    updated_at_api = Column(DateTime)
    
    # System timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    draw = relationship("TournamentDraw", back_populates="matches")
    tournament = relationship("Tournament", back_populates="tournament_matches")

# Update the Tournament model relationships
Tournament.draws = relationship("TournamentDraw", back_populates="tournament")
Tournament.tournament_matches = relationship("TournamentMatch", back_populates="tournament")
    