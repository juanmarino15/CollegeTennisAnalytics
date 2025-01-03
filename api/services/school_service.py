from sqlalchemy.orm import Session
from sqlalchemy import func
from models import SchoolInfo, Team, Player, PlayerRoster

class SchoolService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_schools(self, conference: str = None):
        query = self.db.query(SchoolInfo)
        if conference:
            query = query.filter(func.upper(SchoolInfo.conference) == conference.upper())
        return query.all()

    def get_school(self, school_id: str):
        if school_id:
            upper_school_id = school_id.upper()
            return self.db.query(SchoolInfo).filter(
                func.upper(SchoolInfo.id) == upper_school_id
            ).first()
        return None
    
    def get_school_teams(self, school_id: str):
        """Get both men's and women's teams for a school"""
        if school_id:
            school = self.get_school(school_id)
            if school:
                teams = []
                if school.man_id:
                    men_team = self.db.query(Team).filter(
                        func.upper(Team.id) == school.man_id.upper()
                    ).first()
                    if men_team:
                        teams.append(men_team)
                if school.woman_id:
                    women_team = self.db.query(Team).filter(
                        func.upper(Team.id) == school.woman_id.upper()
                    ).first()
                    if women_team:
                        teams.append(women_team)
                return teams
        return []