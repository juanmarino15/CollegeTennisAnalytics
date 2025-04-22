# api/services/ranking_service.py

from sqlalchemy.orm import Session
from sqlalchemy import func, desc, or_
from typing import List, Optional


from models.models import (
    RankingList, TeamRanking, Team,
    PlayerRankingList, PlayerRanking, Player,DoublesRanking
)

class RankingService:
    def __init__(self, db: Session):
        self.db = db
    
    # Team rankings methods
    def get_team_ranking_lists(self, division_type: Optional[str] = None, gender: Optional[str] = None, limit: Optional[int] = None):
        """Get team ranking lists with optional filters, always sorted by publish_date desc"""
        query = self.db.query(RankingList).filter(
            RankingList.publish_date.isnot(None)  
        ).order_by(desc(RankingList.publish_date))
        
        if division_type:
            query = query.filter(RankingList.division_type == division_type)
        
        if gender:
            query = query.filter(RankingList.gender == gender)
        
        # Only apply limit if one is provided
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_latest_team_ranking_list(self, division_type: str = "DIV1", gender: str = "M"):
        """Get the most recent team ranking list for the given division and gender"""
        return self.db.query(RankingList).filter(
            RankingList.division_type == division_type,
            RankingList.gender == gender,
            RankingList.match_format == "TEAM"
        ).order_by(desc(RankingList.publish_date)).first()
    
    def get_team_rankings(self, ranking_id: str, limit: int = 100):
        """Get team rankings for a specific ranking list"""
        return self.db.query(TeamRanking).filter(
            TeamRanking.ranking_list_id == ranking_id
        ).order_by(TeamRanking.rank).limit(limit).all()
    
    def get_team_ranking_history(self, team_id: str, limit: int = 10):
        """Get ranking history for a specific team"""
        rankings = self.db.query(TeamRanking, RankingList).join(
            RankingList, TeamRanking.ranking_list_id == RankingList.id
        ).filter(
            TeamRanking.team_id == team_id
        ).order_by(desc(RankingList.publish_date)).limit(limit).all()
        
        result = []
        for team_ranking, ranking_list in rankings:
            result.append({
                "ranking_list_id": ranking_list.id,
                "publish_date": ranking_list.publish_date,
                "rank": team_ranking.rank,
                "points": team_ranking.points,
                "wins": team_ranking.wins,
                "losses": team_ranking.losses
            })
        
        return result
    
    # Player rankings methods
    def get_player_ranking_lists(self, division_type: Optional[str] = None, gender: Optional[str] = None, match_format: str = "SINGLES", limit: Optional[int] = None):
        """Get player ranking lists with optional filters, always sorted by publish_date desc"""
        query = self.db.query(PlayerRankingList).filter(
            PlayerRankingList.match_format == match_format,
            # Add filters to exclude null publish dates
            PlayerRankingList.publish_date.isnot(None)
        ).order_by(desc(PlayerRankingList.publish_date))
        
        if division_type:
            query = query.filter(PlayerRankingList.division_type == division_type)
        
        if gender:
            query = query.filter(PlayerRankingList.gender == gender)
        
        # Only apply limit if one is provided
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_latest_player_ranking_list(self, division_type: str = "DIV1", gender: str = "M", match_format: str = "SINGLES"):
        """Get the most recent player ranking list for the given parameters"""
        return self.db.query(PlayerRankingList).filter(
            PlayerRankingList.division_type == division_type,
            PlayerRankingList.gender == gender,
            PlayerRankingList.match_format == match_format
        ).order_by(desc(PlayerRankingList.publish_date)).first()
    
    def get_player_rankings(self, ranking_id: str, limit: int = 100):
        """Get player rankings for a specific ranking list"""
        return self.db.query(PlayerRanking).filter(
            PlayerRanking.ranking_list_id == ranking_id
        ).order_by(PlayerRanking.rank).limit(limit).all()
    
    def get_player_ranking_history(self, player_id: str, match_format: str = "SINGLES", limit: int = 10):
        """Get ranking history for a specific player"""
        rankings = self.db.query(PlayerRanking, PlayerRankingList).join(
            PlayerRankingList, PlayerRanking.ranking_list_id == PlayerRankingList.id
        ).filter(
            PlayerRanking.player_id == player_id,
            PlayerRankingList.match_format == match_format
        ).order_by(desc(PlayerRankingList.publish_date)).limit(limit).all()
        
        result = []
        for player_ranking, ranking_list in rankings:
            result.append({
                "ranking_list_id": ranking_list.id,
                "publish_date": ranking_list.publish_date,
                "rank": player_ranking.rank,
                "points": player_ranking.points,
                "wins": player_ranking.wins,
                "losses": player_ranking.losses,
                "team_id": player_ranking.team_id,
                "team_name": player_ranking.team_name
            })
        
        return result

    def get_doubles_rankings(self, ranking_id: str, limit: int = 100):
        """Get doubles team rankings for a specific ranking list"""
        return self.db.query(DoublesRanking).filter(
            DoublesRanking.ranking_list_id == ranking_id
        ).order_by(DoublesRanking.rank).limit(limit).all()
        
    def get_player_doubles_history(self, player_id: str, limit: int = 10):
        """Get doubles ranking history for a specific player"""
        rankings = self.db.query(DoublesRanking, PlayerRankingList).join(
            PlayerRankingList, DoublesRanking.ranking_list_id == PlayerRankingList.id
        ).filter(
            or_(
                DoublesRanking.player1_id == player_id,
                DoublesRanking.player2_id == player_id
            )
        ).order_by(desc(PlayerRankingList.publish_date)).limit(limit).all()
        
        result = []
        for doubles_ranking, ranking_list in rankings:
            result.append({
                "ranking_list_id": ranking_list.id,
                "publish_date": ranking_list.publish_date,
                "rank": doubles_ranking.rank,
                "points": doubles_ranking.points,
                "wins": doubles_ranking.wins,
                "losses": doubles_ranking.losses,
                "team_id": doubles_ranking.team_id,
                "team_name": doubles_ranking.team_name,
                "partner_id": doubles_ranking.player2_id if doubles_ranking.player1_id == player_id else doubles_ranking.player1_id,
                "partner_name": doubles_ranking.player2_name if doubles_ranking.player1_id == player_id else doubles_ranking.player1_name
            })
        
        return result