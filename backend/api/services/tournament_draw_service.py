# api/services/tournament_draw_service.py
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_, desc, asc, func, text
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, time 

from models.models import Tournament, TournamentDraw, TournamentMatch, TournamentEvent
from api.schemas.tournament_draw import (
    TournamentDrawResponse, TournamentMatchResponse, TournamentInfo,
    TournamentWithDraws, TournamentDrawDetails, TournamentListItem,
    TournamentSearchFilters, TournamentSearchResponse, TournamentBracket,
    BracketPosition, TournamentMatchParticipant, TournamentDrawWithMatches
)

class TournamentDrawService:
    def __init__(self, db: Session):
        self.db = db

    def get_tournaments_list(
        self,
        filters: Optional[TournamentSearchFilters] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "start_date_time",
        sort_order: str = "desc"
    ) -> TournamentSearchResponse:
        """Get paginated list of tournaments with draw counts and event tags"""
        
        query = self.db.query(Tournament)
        
        # Apply filters
        if filters:
            if filters.date_from:
                query = query.filter(Tournament.start_date_time >= filters.date_from)
            if filters.date_to:
                query = query.filter(Tournament.end_date_time <= filters.date_to)
            if filters.tournament_type:
                query = query.filter(Tournament.tournament_type == filters.tournament_type)
            if filters.location:
                query = query.filter(Tournament.location_name.ilike(f"%{filters.location}%"))
            if filters.organization:
                query = query.filter(Tournament.organization_name.ilike(f"%{filters.organization}%"))
            if filters.division:
                query = query.filter(Tournament.organization_division == filters.division)
            if filters.status:
                now = datetime.utcnow()
                if filters.status == "upcoming":
                    query = query.filter(Tournament.start_date_time > now)
                elif filters.status == "current":
                    query = query.filter(
                        and_(
                            Tournament.start_date_time <= now,
                            Tournament.end_date_time >= now
                        )
                    )
                elif filters.status == "completed":
                    query = query.filter(Tournament.end_date_time < now)
        
        # Get total count
        total_count = query.count()
        
        # Apply sorting
        sort_column = getattr(Tournament, sort_by, Tournament.start_date_time)
        if sort_order.lower() == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))
        
        # Apply pagination
        offset = (page - 1) * page_size
        tournaments = query.offset(offset).limit(page_size).all()
        
        # Build response with draw and event information
        tournament_items = []
        for tournament in tournaments:
            # Get draw count from tournament_draws table (case-insensitive)
            draws_count = self.db.query(func.count(TournamentDraw.draw_id)).filter(
                func.upper(TournamentDraw.tournament_id) == func.upper(tournament.tournament_id)
            ).scalar() or 0
            
            # Get event tags from tournament_events table
            events = self._get_tournament_event_tags(tournament.tournament_id)
            
            tournament_items.append(TournamentListItem(
                tournament_id=tournament.tournament_id,
                name=tournament.name,
                start_date_time=tournament.start_date_time,
                end_date_time=tournament.end_date_time,
                location_name=tournament.location_name,
                organization_name=tournament.organization_name,
                organization_division=tournament.organization_division,
                tournament_type=tournament.tournament_type,
                draws_count=draws_count,
                events=events
            ))
        
        return TournamentSearchResponse(
            tournaments=tournament_items,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_next=offset + page_size < total_count,
            has_previous=page > 1
        )

    def _get_tournament_event_tags(self, tournament_id: str) -> List[str]:
        """
        Get aggregated event tags from tournament_events table
        Returns tags like ["Men's and Women's", "Singles and Doubles"]
        """
        # Query tournament_events for this tournament (case-insensitive)
        events = self.db.query(TournamentEvent).filter(
            func.upper(TournamentEvent.tournament_id) == func.upper(tournament_id)
        ).all()
        
        if not events:
            return []
        
        # Check what genders and event types exist
        has_boys = any(e.gender == 'boys' for e in events)
        has_girls = any(e.gender == 'girls' for e in events)
        has_singles = any(e.event_type == 'singles' for e in events)
        has_doubles = any(e.event_type == 'doubles' for e in events)
        
        tags = []
        
        # Add gender tag (convert boys/girls to Men's/Women's for display)
        if has_boys and has_girls:
            tags.append("Men's and Women's")
        elif has_boys:
            tags.append("Men's")
        elif has_girls:
            tags.append("Women's")
        
        # Add event type tag
        if has_singles and has_doubles:
            tags.append("Singles and Doubles")
        elif has_singles:
            tags.append("Singles")
        elif has_doubles:
            tags.append("Doubles")
        
        return tags

    def search_tournaments(
        self, 
        query: Optional[str] = None,
        filters: Optional[TournamentSearchFilters] = None,
        page: int = 1,
        page_size: int = 20
    ) -> TournamentSearchResponse:
        """Search tournaments by name, location, or organization"""
        
        db_query = self.db.query(Tournament)
        
        # Apply text search
        if query:
            search_filter = or_(
                Tournament.name.ilike(f"%{query}%"),
                Tournament.location_name.ilike(f"%{query}%"),
                Tournament.organization_name.ilike(f"%{query}%")
            )
            db_query = db_query.filter(search_filter)
        
        # Apply additional filters
        if filters:
            if filters.date_from:
                db_query = db_query.filter(Tournament.start_date_time >= filters.date_from)
            if filters.date_to:
                db_query = db_query.filter(Tournament.end_date_time <= filters.date_to)
            if filters.tournament_type:
                db_query = db_query.filter(Tournament.tournament_type == filters.tournament_type)
            if filters.division:
                db_query = db_query.filter(Tournament.organization_division == filters.division)
            if filters.status:
                now = datetime.utcnow()
                if filters.status == "upcoming":
                    db_query = db_query.filter(Tournament.start_date_time > now)
                elif filters.status == "current":
                    db_query = db_query.filter(
                        and_(
                            Tournament.start_date_time <= now,
                            Tournament.end_date_time >= now
                        )
                    )
                elif filters.status == "completed":
                    db_query = db_query.filter(Tournament.end_date_time < now)
        
        # Get total count
        total_count = db_query.count()
        
        # Apply pagination and ordering
        offset = (page - 1) * page_size
        tournaments = db_query.order_by(desc(Tournament.start_date_time)).offset(offset).limit(page_size).all()
        
        # Build response items
        tournament_items = []
        for tournament in tournaments:
            # Get draw count (case-insensitive)
            draws_count = self.db.query(func.count(TournamentDraw.draw_id)).filter(
                func.upper(TournamentDraw.tournament_id) == func.upper(tournament.tournament_id)
            ).scalar() or 0
            
            # Get event tags from tournament_events
            events = self._get_tournament_event_tags(tournament.tournament_id)
            
            tournament_items.append(TournamentListItem(
                tournament_id=tournament.tournament_id,
                name=tournament.name,
                start_date_time=tournament.start_date_time,
                end_date_time=tournament.end_date_time,
                location_name=tournament.location_name,
                organization_name=tournament.organization_name,
                organization_division=tournament.organization_division,
                tournament_type=tournament.tournament_type,
                draws_count=draws_count,
                events=events
            ))
        
        return TournamentSearchResponse(
            tournaments=tournament_items,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_next=offset + page_size < total_count,
            has_previous=page > 1
        )

    def get_tournament_with_draws(self, tournament_id: str) -> Optional[TournamentWithDraws]:
        """Get tournament details with all its draws"""
        
        # Convert tournament_id to uppercase for consistent matching
        tournament_id_upper = tournament_id.upper()
        
        tournament = self.db.query(Tournament).filter(
            func.upper(Tournament.tournament_id) == tournament_id_upper
        ).first()
        
        if not tournament:
            return None
        
        # Get draws (case-insensitive)
        draws = self.db.query(TournamentDraw).filter(
            func.upper(TournamentDraw.tournament_id) == tournament_id_upper
        ).order_by(TournamentDraw.gender, TournamentDraw.event_type).all()
        
        draw_responses = [TournamentDrawResponse.from_orm(draw) for draw in draws]
        
        return TournamentWithDraws(
            tournament_id=tournament.tournament_id,
            name=tournament.name,
            start_date_time=tournament.start_date_time,
            end_date_time=tournament.end_date_time,
            location_name=tournament.location_name,
            organization_name=tournament.organization_name,
            tournament_type=tournament.tournament_type,
            draws=draw_responses
        )

    def get_draw_details(self, draw_id: str, stage: Optional[str] = None) -> Optional[TournamentDrawDetails]:
        """Get detailed draw information including matches for a specific stage"""
        
        try:
            # Convert draw_id to uppercase for consistent matching
            draw_id_upper = draw_id.upper()
            
            draw = self.db.query(TournamentDraw).filter(
                func.upper(TournamentDraw.draw_id) == draw_id_upper
            ).first()
            
            if not draw:
                return None
            
            # Get tournament info
            tournament = self.db.query(Tournament).filter(
                func.upper(Tournament.tournament_id) == func.upper(draw.tournament_id)
            ).first()
            
            # Get matches for this draw and specific stage (case-insensitive)
            matches_query = self.db.query(TournamentMatch).filter(
                func.upper(TournamentMatch.draw_id) == draw_id_upper
            )
            
            # Filter by stage if provided
            if stage:
                matches_query = matches_query.filter(TournamentMatch.stage == stage)
            
            matches = matches_query.order_by(
                TournamentMatch.round_number, 
                TournamentMatch.round_position
            ).all()
            
            # Convert matches to response format
            match_responses = []
            for match in matches:
                side1 = TournamentMatchParticipant(
                    participant_id=match.side1_participant_id,
                    participant_name=match.side1_participant_name,
                    draw_position=match.side1_draw_position,
                    seed_number=match.side1_seed_number,
                    school_name=match.side1_school_name,
                    school_id=match.side1_school_id,
                    player1_id=match.side1_player1_id,
                    player1_name=match.side1_player1_name,
                    player2_id=match.side1_player2_id,
                    player2_name=match.side1_player2_name
                )
                
                side2 = TournamentMatchParticipant(
                    participant_id=match.side2_participant_id,
                    participant_name=match.side2_participant_name,
                    draw_position=match.side2_draw_position,
                    seed_number=match.side2_seed_number,
                    school_name=match.side2_school_name,
                    school_id=match.side2_school_id,
                    player1_id=match.side2_player1_id,
                    player1_name=match.side2_player1_name,
                    player2_id=match.side2_player2_id,
                    player2_name=match.side2_player2_name
                )
                
                # Fix the scheduled_time conversion for the datetime issue
                scheduled_time_fixed = None
                if match.scheduled_time is not None:
                    if match.scheduled_date and hasattr(match.scheduled_time, 'hour'):
                        # It's a time object, combine with date
                        from datetime import datetime, time
                        scheduled_time_fixed = datetime.combine(match.scheduled_date, match.scheduled_time)
                    else:
                        # It's already a datetime or None
                        scheduled_time_fixed = match.scheduled_time
                
                match_response = TournamentMatchResponse(
                id=match.id,
                match_up_id=match.match_up_id,
                draw_id=match.draw_id,
                tournament_id=match.tournament_id,
                event_id=match.event_id,
                round_name=match.round_name,
                round_number=match.round_number,
                round_position=match.round_position,
                match_type=match.match_type,
                match_format=match.match_format,
                match_status=match.match_status,
                stage=match.stage,
                structure_name=match.structure_name,
                side1=side1,
                side2=side2,
                winning_side=match.winning_side,
                winner_participant_id=match.winner_participant_id,
                winner_participant_name=match.winner_participant_name,
                score_side1=match.score_side1,
                score_side2=match.score_side2,
                scheduled_date=None,  # Set to None for now
                scheduled_time=None,  # Set to None for now
                venue_name=match.venue_name,
                created_at_api=match.created_at_api,
                updated_at_api=match.updated_at_api,
                created_at=match.created_at,
                updated_at=match.updated_at
            )
                match_responses.append(match_response)
            
            # Calculate statistics
            total_matches = len(matches)
            completed_matches = len([m for m in matches if m.match_status == "COMPLETED"])
            scheduled_matches = len([m for m in matches if m.match_status == "SCHEDULED"])
            
            # Count unique participants
            participants = set()
            for match in matches:
                if match.side1_participant_id:
                    participants.add(match.side1_participant_id)
                if match.side2_participant_id:
                    participants.add(match.side2_participant_id)
            
            tournament_info = None
            if tournament:
                tournament_info = TournamentInfo(
                    tournament_id=tournament.tournament_id,
                    name=tournament.name,
                    start_date_time=tournament.start_date_time,
                    end_date_time=tournament.end_date_time,
                    location_name=tournament.location_name,
                    organization_name=tournament.organization_name,
                    tournament_type=tournament.tournament_type
                )
            
            return TournamentDrawDetails(
                draw_id=draw.draw_id,
                tournament_id=draw.tournament_id,
                event_id=draw.event_id,
                draw_name=draw.draw_name,
                draw_type=draw.draw_type,
                draw_size=draw.draw_size,
                event_type=draw.event_type,
                gender=draw.gender,
                draw_completed=draw.draw_completed,
                draw_active=draw.draw_active,
                match_up_format=draw.match_up_format,
                updated_at_api=draw.updated_at_api,
                created_at=draw.created_at,
                updated_at=draw.updated_at,
                tournament=tournament_info,
                matches=match_responses,
                total_matches=total_matches,
                completed_matches=completed_matches,
                scheduled_matches=scheduled_matches,
                participants_count=len(participants)
            )
            
        except Exception as e:
            print(f"Error in get_draw_details for draw {draw_id}, stage {stage}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def get_tournament_draws(self, tournament_id: str) -> List[TournamentDrawResponse]:
        """Get all draws for a specific tournament, separated by stage"""
        
        # Convert tournament_id to uppercase for consistent matching
        tournament_id_upper = tournament_id.upper()
        
        # Get base draws
        base_draws = self.db.query(TournamentDraw).filter(
            func.upper(TournamentDraw.tournament_id) == tournament_id_upper
        ).order_by(TournamentDraw.gender, TournamentDraw.event_type).all()
        
        draw_responses = []
        
        for base_draw in base_draws:
            # Get all unique stages for this draw
            stages = self.db.query(TournamentMatch.stage).filter(
                func.upper(TournamentMatch.draw_id) == func.upper(base_draw.draw_id),
                TournamentMatch.stage.isnot(None)
            ).distinct().all()
            
            stage_list = [stage[0] for stage in stages if stage[0]]
            
            if not stage_list:
                # No stages found, return the base draw
                draw_responses.append(TournamentDrawResponse.from_orm(base_draw))
            else:
                # Create a separate draw response for each stage
                for stage in stage_list:
                    # Count matches for this stage
                    stage_match_count = self.db.query(func.count(TournamentMatch.id)).filter(
                        func.upper(TournamentMatch.draw_id) == func.upper(base_draw.draw_id),
                        TournamentMatch.stage == stage
                    ).scalar() or 0
                    
                    # Create modified draw name to include stage
                    stage_suffix = ""
                    if stage.upper() == "CONSOLATION":
                        stage_suffix = " - Consolation"
                    elif stage.upper() == "QUALIFYING":
                        stage_suffix = " - Qualifying"
                    elif stage.upper() == "MAIN":
                        stage_suffix = ""  # Main draw doesn't need suffix
                    else:
                        stage_suffix = f" - {stage.title()}"
                    
                    # Create a new draw response with modified name and size
                    draw_response = TournamentDrawResponse(
                        draw_id=base_draw.draw_id,
                        tournament_id=base_draw.tournament_id,
                        event_id=base_draw.event_id,
                        draw_name=base_draw.draw_name + stage_suffix,
                        draw_type=base_draw.draw_type,
                        draw_size=stage_match_count * 2,  # Rough estimate
                        event_type=base_draw.event_type,
                        gender=base_draw.gender,
                        draw_completed=base_draw.draw_completed,
                        draw_active=base_draw.draw_active,
                        match_up_format=base_draw.match_up_format,
                        updated_at_api=base_draw.updated_at_api,
                        created_at=base_draw.created_at,
                        updated_at=base_draw.updated_at,
                        stage=stage  # Add stage to the response
                    )
                    draw_responses.append(draw_response)
        
        return draw_responses

    def get_draw_bracket(self, draw_id: str) -> Optional[TournamentBracket]:
        """Get bracket visualization data for a draw"""
        
        # Convert draw_id to uppercase for consistent matching
        draw_id_upper = draw_id.upper()
        
        draw = self.db.query(TournamentDraw).filter(
            func.upper(TournamentDraw.draw_id) == draw_id_upper
        ).first()
        
        if not draw:
            return None
        
        # Get all matches for this draw to build bracket positions (case-insensitive)
        matches = self.db.query(TournamentMatch).filter(
            func.upper(TournamentMatch.draw_id) == draw_id_upper
        ).order_by(TournamentMatch.round_number, TournamentMatch.round_position).all()
        
        # Build bracket positions from matches
        positions = []
        position_map = {}
        
        for match in matches:
            # Add side 1 participant
            if match.side1_participant_id and match.side1_draw_position:
                pos = BracketPosition(
                    draw_position=match.side1_draw_position,
                    round_number=match.round_number or 1,
                    participant_name=match.side1_participant_name,
                    participant_id=match.side1_participant_id,
                    seed_number=match.side1_seed_number,
                    school_name=match.side1_school_name,
                    is_winner=(match.winning_side == 1),
                    is_bye=False
                )
                if match.side1_draw_position not in position_map:
                    positions.append(pos)
                    position_map[match.side1_draw_position] = pos
            
            # Add side 2 participant
            if match.side2_participant_id and match.side2_draw_position:
                pos = BracketPosition(
                    draw_position=match.side2_draw_position,
                    round_number=match.round_number or 1,
                    participant_name=match.side2_participant_name,
                    participant_id=match.side2_participant_id,
                    seed_number=match.side2_seed_number,
                    school_name=match.side2_school_name,
                    is_winner=(match.winning_side == 2),
                    is_bye=False
                )
                if match.side2_draw_position not in position_map:
                    positions.append(pos)
                    position_map[match.side2_draw_position] = pos
        
        # Calculate number of rounds based on draw size
        import math
        rounds = int(math.log2(draw.draw_size)) if draw.draw_size and draw.draw_size > 0 else 1
        
        return TournamentBracket(
            draw_id=draw.draw_id,
            draw_name=draw.draw_name or "",
            draw_type=draw.draw_type or "",
            draw_size=draw.draw_size or 0,
            rounds=rounds,
            positions=sorted(positions, key=lambda x: x.draw_position)
        )
    def get_draw_stages(self, draw_id: str) -> List[str]:
        """Get all available stages for a specific draw"""
        
        draw_id_upper = draw_id.upper()
        
        stages = self.db.query(TournamentMatch.stage).filter(
            func.upper(TournamentMatch.draw_id) == draw_id_upper,
            TournamentMatch.stage.isnot(None)
        ).distinct().all()
        
        return [stage[0] for stage in stages if stage[0]]