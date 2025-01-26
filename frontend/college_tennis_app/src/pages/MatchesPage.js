import React, { useState, useEffect, useMemo} from 'react';
import { Calendar, Clock, ChevronRight,ArrowUpDown } from 'lucide-react';
import { api } from '../services/api';
import { useNavigate } from 'react-router-dom';

// TeamLogo component
const TeamLogo = ({ teamId }) => {
  const size = "w-8 h-8";
  const [hasError, setHasError] = useState(false);

  // Only try to load image if we have a teamId
  if (!teamId || hasError) {
    return (
      <div className={`${size} flex items-center justify-center text-gray-400 dark:text-gray-600`}>
        {/* use a placeholder icon here */}
        <span className="text-xs">Logo</span>
      </div>
    );
  }

  return (
    <div className={`${size} flex items-center justify-center mix-blend-multiply dark:mix-blend-normal`}>
      <img
        src={`http://localhost:8000/api/v1/teams/${teamId}/logo`}
        alt="Team Logo"
        className="object-contain mix-blend-multiply dark:mix-blend-normal"
        onError={() => setHasError(true)}
      />
    </div>
  );
};


const MatchesPage = () => {
 const [matches, setMatches] = useState([]);
 const [teams, setTeams] = useState({});
 const [loading, setLoading] = useState(true);
 const [error, setError] = useState(null);
 const [selectedDate, setSelectedDate] = useState(new Date());
 const [matchScores, setMatchScores] = useState({}); //match scores
 const [availableConferences, setAvailableConferences] = useState(new Set());
 const navigate = useNavigate();
 const [filters, setFilters] = useState({
  gender: '',
  conference: '',
  sort: 'time-asc'
  });

// Sort matches by time
const sortedMatches = useMemo(() => {
  return [...matches].sort((a, b) => {
    const timeA = new Date(a.scheduled_time);
    const timeB = new Date(b.scheduled_time);
    
    switch (filters.sort) {
      case 'time-desc':
        return timeB - timeA;
      case 'conference':
        // Sort conference matches first
        if (a.is_conference_match && !b.is_conference_match) return -1;
        if (!a.is_conference_match && b.is_conference_match) return 1;
        return timeA - timeB;
      case 'completed':
        // Sort completed matches first
        if (a.completed && !b.completed) return -1;
        if (!a.completed && b.completed) return 1;
        return timeA - timeB;
      case 'time-asc':
      default:
        return timeA - timeB;
    }
  });
}, [matches, filters.sort]);

// Helper function to format conference name
const formatConferenceName = (conference) => {
  return conference.replace(/_/g, ' ');
};

 // Helper function to format datetime
 const formatMatchTime = (dateTimeStr, timezone) => {
   try {
     const date = new Date(dateTimeStr);
     return date.toLocaleTimeString('en-US', {
       hour: '2-digit',
       minute: '2-digit',
       timeZoneName: 'short',
       timeZone: timezone

     });
   } catch (e) {
     return 'Time TBD';
   }
 };
 

 // Fetch team data
const fetchTeams = async (matchesData) => {
  const teamIds = new Set();
  const conferences = new Set();  // Add this
  
  matchesData.forEach(match => {
    teamIds.add(match.home_team_id);
    teamIds.add(match.away_team_id);
  });

  const teamsMap = {};
  await Promise.all([...teamIds].map(async (teamId) => {
    try {
      const team = await api.teams.getById(teamId);
      teamsMap[teamId] = team;
      if (team.conference) {  // Add this
        conferences.add(team.conference);
      }
    } catch (error) {
      console.error(`Failed to fetch team ${teamId}:`, error);
    }
  }));
  
  setAvailableConferences(conferences);  // Add this
  setTeams(teamsMap);
};

// Fetch matches with teams
useEffect(() => {
  const abortController = new AbortController();
  let isMounted = true;

  const fetchData = async () => {
    try {
      if (!isMounted) return;
      setLoading(true);
      
      const dateStr = selectedDate.toISOString().split('T')[0];
      let matchesData = await api.matches.getAll(dateStr, abortController.signal);
      
      if (!isMounted) return;

      // First get all team data
      const teamIds = new Set();
      matchesData.forEach(match => {
        teamIds.add(match.home_team_id);
        teamIds.add(match.away_team_id);
      });

      // Fetch all team data in parallel
      const teamPromises = Array.from(teamIds).map(teamId => 
        api.teams.getById(teamId, abortController.signal)
      );
      const teamResults = await Promise.all(teamPromises);
      
      if (!isMounted) return;

      // Create teams map
      const teamsMap = {};
      const conferences = new Set();
      teamResults.forEach(team => {
        teamsMap[team.id] = team;
        if (team.conference) {
          conferences.add(team.conference);
        }
      });

      // Apply filters
      let filteredMatches = matchesData;
      if (filters.gender) {
        filteredMatches = filteredMatches.filter(match => match.gender === filters.gender);
      }
      if (filters.conference) {
        filteredMatches = filteredMatches.filter(match => {
          const homeTeam = teamsMap[match.home_team_id];
          const awayTeam = teamsMap[match.away_team_id];
          return homeTeam?.conference === filters.conference || awayTeam?.conference === filters.conference;
        });
      }

      // Fetch scores for completed matches in parallel
      const completedMatches = filteredMatches.filter(match => match.completed);
      const scorePromises = completedMatches.map(match => 
        api.matches.getScore(match.id, abortController.signal)
      );
      const scoreResults = await Promise.all(scorePromises);
      
      if (!isMounted) return;

      // Create scores map
      const scoresMap = {};
      completedMatches.forEach((match, index) => {
        scoresMap[match.id] = scoreResults[index];
      });

      // Set all state at once
      setMatches(filteredMatches);
      setTeams(teamsMap);
      setMatchScores(scoresMap);
      setAvailableConferences(conferences);
      
    } catch (err) {
      if (!isMounted) return;
      if (err.name === 'AbortError') return;
      setError(err.message);
    } finally {
      if (!isMounted) return;
      setLoading(false);
    }
  };

  fetchData();

  return () => {
    isMounted = false;
    abortController.abort();
  };
}, [selectedDate, filters]);

 // Get team name helper
 const getTeamName = (teamId) => {
   const team = teams[teamId];
   if (!team) return 'Loading...';
   
   // Remove (M) or (F) from the end of the name
   return team.name
 };

 return (
  <div className="py-4 space-y-4">
    {/* Date and Filters Section */}
    <div className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
      <div className="flex flex-col sm:flex-row flex-wrap gap-4">
        {/* Date selector - Full width on mobile, auto on larger screens */}
        <div className="w-full sm:w-auto">
          <div className="flex items-center gap-2">
            <Calendar className="w-5 h-5 text-primary-500 flex-shrink-0" />
            <input
              type="date"
              value={selectedDate.toISOString().split('T')[0]}
              onChange={(e) => setSelectedDate(new Date(e.target.value))}
              className="w-full sm:w-auto bg-transparent border border-gray-200 dark:border-dark-border rounded px-2 py-1
                        text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
            />
          </div>
        </div>

        {/* Filters Container - Grid on mobile, flex-row on larger screens */}
        <div className="grid grid-cols-1 sm:flex sm:flex-row gap-4">
          {/* Gender Filter */}
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap min-w-[60px]">Gender:</label>
            <select
              value={filters.gender}
              onChange={(e) => setFilters(prev => ({ ...prev, gender: e.target.value }))}
              className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-2 py-1
                        text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All</option>
              <option value="MALE">Men</option>
              <option value="FEMALE">Women</option>
            </select>
          </div>

          {/* Conference Filter */}
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap min-w-[60px]">Conference:</label>
            <select
              value={filters.conference}
              onChange={(e) => setFilters(prev => ({ ...prev, conference: e.target.value }))}
              className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-2 py-1
                        text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All Matches</option>
              {[...availableConferences].sort().map(conf => (
                <option key={conf} value={conf}>
                  {formatConferenceName(conf)}
                </option>
              ))}
            </select>
          </div>

          {/* Sort Filter */}
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap min-w-[60px] flex items-center">
              <ArrowUpDown className="w-4 h-4 mr-1" />
              Sort:
            </label>
            <select
              value={filters.sort}
              onChange={(e) => setFilters(prev => ({ ...prev, sort: e.target.value }))}
              className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-2 py-1
                        text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
            >
              <option value="time-asc">Start Time (Earliest)</option>
              <option value="time-desc">Start Time (Latest)</option>
              <option value="conference">Conference First</option>
              <option value="completed">Completed First</option>
            </select>
          </div>
        </div>
      </div>
    </div>

    {/* Matches List */}
    <div className="space-y-4">
      {loading ? (
        [...Array(3)].map((_, i) => (
          <div key={i} className="animate-pulse bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
            <div className="h-6 bg-gray-200 dark:bg-dark-border rounded w-3/4 mb-3"></div>
            <div className="h-4 bg-gray-200 dark:bg-dark-border rounded w-1/2"></div>
          </div>
        ))
      ) : error ? (
        <div className="bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 p-4 rounded-lg">
          {error}
        </div>
      ) : sortedMatches.length === 0 ? (
        <div className="bg-white dark:bg-dark-card rounded-lg p-8 text-center text-gray-500 dark:text-dark-text-dim">
          No matches scheduled for this date
        </div>
      ) : (
        sortedMatches.map(match => (
          <div key={match.id} 
            onClick={() => navigate(`/matches/${match.id}`)}
            className={`relative bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg
                        hover:shadow-xl transition-shadow cursor-pointer
                        ${match.is_conference_match ? 'border-l-4 border-primary-500 dark:border-primary-400' : ''}`}>
            <div className="flex flex-col">
              {/* Teams and Time Row */}
              <div className="flex justify-center items-center gap-8">
                {/* Home Team */}
                <div className="flex items-center gap-2 w-1/3 justify-end">
                  <span className={`font-medium text-gray-900 dark:text-dark-text text-right
                                ${match.is_conference_match ? 'font-semibold' : ''}`}>
                    {getTeamName(match.home_team_id)}
                  </span>
                  <TeamLogo teamId={match.home_team_id} />
                </div>

                {/* Time/Score Section */}
                <div className="flex flex-col items-center w-1/3">
                  {match.completed ? (
                    <span className="text-lg font-medium text-gray-900 dark:text-dark-text">
                      {matchScores[match.id] ? 
                        `${matchScores[match.id].home_team_score} - ${matchScores[match.id].away_team_score}` 
                        : 'vs'}
                    </span>
                  ) : (
                    <>
                      <span className="text-lg font-medium text-gray-900 dark:text-dark-text">
                        {(() => {
                          try {
                            if (!match.scheduled_time) return 'TBD';
                            const date = new Date(match.scheduled_time + 'Z');
                            if (isNaN(date.getTime())) return 'TBD';
                            return new Date(match.scheduled_time + 'Z').toLocaleTimeString('en-US', {
                              hour: '2-digit',
                              minute: '2-digit',
                              hour12: true,
                              timeZone: match.timezone
                            });
                          } catch (e) {
                            return 'TBD';
                          }
                        })()}
                      </span>
                      <span className="text-sm text-gray-500 dark:text-gray-400">
                        {(() => {
                          try {
                            if (!match.scheduled_time) return '';
                            const date = new Date(match.scheduled_time + 'Z');
                            if (isNaN(date.getTime())) return '';
                            return formatMatchTime(match.scheduled_time, match.timezone).split(' ').pop()
                          } catch (e) {
                            return '';
                          }
                        })()}
                        
                      </span>
                    </>
                  )}
                  {match.completed && (
                    <span className="text-xs px-2 py-0.5 mt-1 rounded-full inline-flex items-center
                                  bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
                      Final
                    </span>
                  )}
                </div>

                {/* Away Team */}
                <div className="flex items-center gap-2 w-1/3">
                  <TeamLogo teamId={match.away_team_id} />
                  <span className={`font-medium text-gray-900 dark:text-dark-text
                                ${match.is_conference_match ? 'font-semibold' : ''}`}>
                    {getTeamName(match.away_team_id)}
                  </span>
                </div>
              </div>

              {/* Conference Match Tag */}
              {match.is_conference_match && (
                <div className="absolute top-2 right-2">
                  <span className="text-xs bg-primary-100 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 px-2 py-0.5 rounded-full inline-flex items-center">
                    Conference
                  </span>
                </div>
              )}
            </div>
          </div>
        ))
      )}
    </div>
  </div>
);
};

export default MatchesPage;