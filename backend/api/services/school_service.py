from sqlalchemy.orm import Session
from sqlalchemy import func
from models import SchoolInfo, Team, Player, PlayerRoster

class SchoolService:
    def __init__(self, db: Session):
        self.db = db
    
    def _school_to_dict(self, school):
        return {
            "id": school.id,
            "name": school.name,
            "conference": school.conference,
            "ita_region": school.ita_region,
            "ranking_award_region": school.ranking_award_region,
            "usta_section": school.usta_section,
            "man_id": school.man_id,
            "woman_id": school.woman_id,
            "division": school.division,
            "mailing_address": school.mailing_address,
            "city": school.city,
            "state": school.state,
            "zip_code": school.zip_code,
            "team_type": school.team_type
        }

    def _team_to_dict(self, team):
        return {
            "id": team.id,
            "name": team.name,
            "abbreviation": team.abbreviation,
            "division": team.division,
            "conference": team.conference,
            "region": team.region,
            "typename": team.typename,
            "gender": team.gender
        }
    
    def get_schools(self, conference: str = None):
        query = self.db.query(SchoolInfo)
        if conference:
            query = query.filter(func.upper(SchoolInfo.conference) == conference.upper())
        schools = query.all()
        return [self._school_to_dict(school) for school in schools]

    def get_school(self, school_id: str):
        if school_id:
            upper_school_id = school_id.upper()
            school = self.db.query(SchoolInfo).filter(
                func.upper(SchoolInfo.id) == upper_school_id
            ).first()
            return self._school_to_dict(school) if school else None
        return None
    
    def get_school_teams(self, school_id: str):
        """Get both men's and women's teams for a school"""
        if school_id:
            school = self.get_school(school_id)
            if school:
                teams = []
                if school["man_id"]:
                    men_team = self.db.query(Team).filter(
                        func.upper(Team.id) == school["man_id"].upper()
                    ).first()
                    if men_team:
                        teams.append(self._team_to_dict(men_team))
                if school["woman_id"]:
                    women_team = self.db.query(Team).filter(
                        func.upper(Team.id) == school["woman_id"].upper()
                    ).first()
                    if women_team:
                        teams.append(self._team_to_dict(women_team))
                return teams
        return []