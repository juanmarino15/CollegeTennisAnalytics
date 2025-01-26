import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { Calendar, MapPin, User, Clock } from 'lucide-react';
import { api } from '../services/api';

// Reuse TeamLogo component for consistency
const TeamLogo = ({ teamId, size = "w-16 h-16" }) => {
    const [hasError, setHasError] = useState(false);
  
    if (!teamId || hasError) {
      return (
        <div className={`${size} flex items-center justify-center text-gray-400 dark:text-gray-600`}>
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

const MatchDetailsPage = () => {
  const { matchId } = useParams();
  const [match, setMatch] = useState(null);
  const [lineup, setLineup] = useState([]);
  const [teams, setTeams] = useState({
    home: null,
    away: null
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [matchScore, setMatchScore] = useState(null);

  useEffect(() => {
    const abortController = new AbortController();
    let isMounted = true;
  
    const fetchMatchDetails = async () => {
      try {
        if (!isMounted) return;
        setLoading(true);
        
        // First fetch match data
        const matchData = await api.matches.getById(matchId, abortController.signal);
        if (!isMounted) return;
        
        // Then fetch teams, lineup, and score in parallel
        const [homeTeam, awayTeam, lineupData, scoreData] = await Promise.all([
          api.teams.getById(matchData.home_team_id, abortController.signal),
          api.teams.getById(matchData.away_team_id, abortController.signal),
          matchData.completed ? api.matches.getLineup(matchId, abortController.signal) : Promise.resolve([]),
          matchData.completed ? api.matches.getScore(matchId, abortController.signal) : Promise.resolve(null)
        ]);
  
        if (!isMounted) return;
        
        setMatch(matchData);
        setTeams({ home: homeTeam, away: awayTeam });
        setLineup(lineupData);
        setMatchScore(scoreData);
        
      } catch (err) {
        if (!isMounted) return;
        if (err.name === 'AbortError') return;
        setError(err.message);
      } finally {
        if (!isMounted) return;
        setLoading(false);
      }
    };
  
    fetchMatchDetails();
  
    return () => {
      isMounted = false;
      abortController.abort();
    };
  }, [matchId]);

  
  // Separate doubles and singles matches
  const doublesMatches = lineup.filter(match => match.match_type === 'DOUBLES')
    .sort((a, b) => a.position - b.position);
  const singlesMatches = lineup.filter(match => match.match_type === 'SINGLES')
    .sort((a, b) => a.position - b.position);

  if (loading) {
    return (
      <div className="max-w-4xl mx-auto p-4">
        <div className="animate-pulse space-y-4">
          <div className="h-40 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
          <div className="h-60 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-4xl mx-auto p-4">
        <div className="bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 p-4 rounded-lg">
          {error}
        </div>
      </div>
    );
  }

  if (!match || !teams.home || !teams.away) {
    return null;
  }

  return (
    <div className="max-w-4xl mx-auto py-4 px-4 space-y-6">
      {/* Match Header Card */}
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
      <div className="flex justify-between items-center mb-6">
    {/* Home Team */}
    <div className="flex flex-col items-center flex-1">
      <TeamLogo teamId={match.home_team_id} />
      <div className="mt-2">
        <div className={`text-lg font-medium text-gray-900 dark:text-dark-text text-center
                     ${match.is_conference_match ? 'font-semibold' : ''}`}>
          {teams.home.name}
        </div>
        {teams.home.conference && (
          <div className="text-sm text-gray-500 dark:text-gray-400 text-center">
            {teams.home.conference.replace(/_/g, ' ')}
          </div>
        )}
      </div>
    </div>

    {/* Score/VS Section */}
   {/* Score/VS Section */}
<div className="flex flex-col items-center mx-4">
  <span className="text-2xl font-bold text-gray-900 dark:text-dark-text">
    {match.completed && matchScore ? 
      `${matchScore.home_team_score} - ${matchScore.away_team_score}` 
      : 'vs'
    }
  </span>
  {match.completed && (
    <span className="text-xs px-2 py-0.5 mt-1 rounded-full inline-flex items-center
                   bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
      Final
    </span>
  )}
  {!match.completed && match.scheduled_time && (
    <span className="text-sm text-gray-600 dark:text-gray-400 mt-1">
      {new Date(match.scheduled_time + 'Z').toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: true,
        timeZone: match.timezone
      })}
    </span>
  )}
</div>

    {/* Away Team */}
    <div className="flex flex-col items-center flex-1">
      <TeamLogo teamId={match.away_team_id} />
      <div className="mt-2">
        <div className={`text-lg font-medium text-gray-900 dark:text-dark-text text-center
                     ${match.is_conference_match ? 'font-semibold' : ''}`}>
          {teams.away.name}
        </div>
        {teams.away.conference && (
          <div className="text-sm text-gray-500 dark:text-gray-400 text-center">
            {teams.away.conference.replace(/_/g, ' ')}
          </div>
        )}
      </div>
    </div>
  </div>

       {/* Match Details Row */}
        <div className="flex justify-center gap-6 text-gray-600 dark:text-gray-400">
            <div className="flex items-center gap-2">
            <Calendar className="w-5 h-5" />
            {new Date(match.start_date).toLocaleDateString('en-US', {
                weekday: 'long',
                year: 'numeric',
                month: 'long',
                day: 'numeric'
            })}
            </div>
            {match.scheduled_time && (
            <div className="flex items-center gap-2">
                <Clock className="w-5 h-5" />
                <span>
                {new Date(match.scheduled_time + 'Z').toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: true,
                    timeZone: match.timezone
                })}
                {' '}
                {/* {match.timezone} */}
                {formatMatchTime(match.scheduled_time, match.timezone).split(' ').pop()}
                </span>
            </div>
            )}
        </div>

        {match.is_conference_match && (
          <div className="mt-4 text-center">
            <span className="bg-primary-100 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 
                           text-sm font-medium px-3 py-1 rounded-full">
              Conference Match
            </span>
          </div>
        )}
      </div>

      {/* Match Results */}
      {match.completed && lineup.length > 0 && (
        <div className="space-y-6">
          {/* Doubles Section */}
          {doublesMatches.length > 0 && (
            <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
              <h2 className="text-xl font-bold mb-4 text-gray-900 dark:text-dark-text">
                Doubles
              </h2>
              <div className="space-y-4">
                {doublesMatches.map((match) => (
                  <div key={match.id} 
                       className="border dark:border-dark-border rounded-lg p-4">
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="font-semibold text-gray-900 dark:text-dark-text">
                          #{match.position}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                          {match.side1_player1_id} / {match.side1_player2_id}
                          <br />
                          vs
                          <br />
                          {match.side2_player1_id} / {match.side2_player2_id}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="font-semibold text-gray-900 dark:text-dark-text">
                          {match.side1_score}
                        </div>
                        <div className="text-xs px-2 py-0.5 mt-1 rounded-full inline-flex 
                                    bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
                          {match.side1_won ? 'W' : 'L'}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Singles Section */}
          {singlesMatches.length > 0 && (
            <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
              <h2 className="text-xl font-bold mb-4 text-gray-900 dark:text-dark-text">
                Singles
              </h2>
              <div className="space-y-4">
                {singlesMatches.map((match) => (
                  <div key={match.id} 
                       className="border dark:border-dark-border rounded-lg p-4">
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="font-semibold text-gray-900 dark:text-dark-text">
                          #{match.position}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                          {match.side1_player1_id}
                          <br />
                          vs
                          <br />
                          {match.side2_player1_id}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="font-semibold text-gray-900 dark:text-dark-text">
                          {match.side1_score}
                        </div>
                        <div className="text-xs px-2 py-0.5 mt-1 rounded-full inline-flex 
                                    bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
                          {match.side1_won ? 'W' : 'L'}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default MatchDetailsPage;