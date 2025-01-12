# api/services/stats_service.py
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, extract
from models import (
    PlayerMatch, 
    PlayerMatchParticipant,
    Match,
    MatchTeam
)
from datetime import datetime

class StatsService:
    def __init__(self, db: Session):
        self.db = db
    
    def _player_stats_to_dict(self, stats):
        """Convert player stats to standardized dictionary"""
        return {
            "singles_wins": stats['singles_wins'],
            "singles_losses": stats['singles_losses'],
            "doubles_wins": stats['doubles_wins'],
            "doubles_losses": stats['doubles_losses'],
            "total_matches": stats['total_matches']
        }

    def _team_stats_to_dict(self, stats):
        """Convert team stats to standardized dictionary"""
        return {
            "total_wins": stats['total_wins'],
            "total_losses": stats['total_losses'],
            "home_wins": stats['home_wins'],
            "home_losses": stats['home_losses'],
            "away_wins": stats['away_wins'],
            "away_losses": stats['away_losses'],
            "total_matches": stats['total_matches']
        }

    def get_player_stats(self, player_id: str, season: str = None):
        """Get player's win/loss record and other statistics"""
        if player_id:
            upper_player_id = player_id.upper()
            
            # Base query for matches
            query = self.db.query(PlayerMatchParticipant).join(PlayerMatch)
            
            # Add season filter if provided
            if season:
                year = int(season)
                season_start = datetime(year, 8, 1)
                season_end = datetime(year + 1, 7, 31)
                query = query.filter(
                    PlayerMatch.start_time.between(season_start, season_end)
                )
            
            # Filter for this player
            matches = query.filter(
                func.upper(PlayerMatchParticipant.person_id) == upper_player_id
            ).all()
            
            stats = {
                'singles_wins': 0,
                'singles_losses': 0,
                'doubles_wins': 0,
                'doubles_losses': 0,
                'total_matches': len(matches)
            }
            
            for match_participant in matches:
                match = match_participant.match
                if match.match_type == 'SINGLES':
                    if match_participant.is_winner:
                        stats['singles_wins'] += 1
                    else:
                        stats['singles_losses'] += 1
                elif match.match_type == 'DOUBLES':
                    if match_participant.is_winner:
                        stats['doubles_wins'] += 1
                    else:
                        stats['doubles_losses'] += 1
            
            return self._player_stats_to_dict(stats)
        return None

    def get_team_stats(self, team_id: str, season: str = None):
        """Get team's overall record and stats"""
        if team_id:
            upper_team_id = team_id.upper()
            
            # Base query
            query = self.db.query(MatchTeam).join(Match)
            
            # Add season filter if provided
            if season:
                year = int(season)
                season_start = datetime(year, 8, 1)
                season_end = datetime(year + 1, 7, 31)
                query = query.filter(
                    Match.start_date.between(season_start, season_end)
                )
            
            # Get matches for this team
            matches = query.filter(
                func.upper(MatchTeam.team_id) == upper_team_id
            ).all()
            
            stats = {
                'total_wins': 0,
                'total_losses': 0,
                'home_wins': 0,
                'home_losses': 0,
                'away_wins': 0,
                'away_losses': 0,
                'total_matches': len(matches)
            }
            
            for match in matches:
                if match.is_home_team:
                    if match.did_win:
                        stats['home_wins'] += 1
                        stats['total_wins'] += 1
                    else:
                        stats['home_losses'] += 1
                        stats['total_losses'] += 1
                else:
                    if match.did_win:
                        stats['away_wins'] += 1
                        stats['total_wins'] += 1
                    else:
                        stats['away_losses'] += 1
                        stats['total_losses'] += 1
            
            return self._team_stats_to_dict(stats)
        return None