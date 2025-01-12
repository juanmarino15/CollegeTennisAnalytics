import React, { useState, useEffect } from 'react';
import { Calendar, Clock, ChevronRight } from 'lucide-react';
import { api } from '../services/api';

// TeamLogo component
const TeamLogo = ({ teamId }) => {
  const size = "w-8 h-8";
  const [hasError, setHasError] = useState(false);

  // Only try to load image if we have a teamId
  if (!teamId || hasError) {
    return (
      <div className={`${size} flex items-center justify-center text-gray-400 dark:text-gray-600`}>
        {/* You could use a placeholder icon here if you want */}
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

 // Helper function to format datetime
 const formatMatchTime = (dateTimeStr, timezone) => {
   try {
     const date = new Date(dateTimeStr);
     return date.toLocaleTimeString('en-US', {
       hour: '2-digit',
       minute: '2-digit',
       timeZoneName: 'short'
     });
   } catch (e) {
     return 'Time TBD';
   }
 };
 

 // Fetch team data
 const fetchTeams = async (matchesData) => {
   const teamIds = new Set();
   matchesData.forEach(match => {
     teamIds.add(match.home_team_id);
     teamIds.add(match.away_team_id);
   });

   const teamsMap = {};
   await Promise.all([...teamIds].map(async (teamId) => {
     try {
       const team = await api.teams.getById(teamId);
       teamsMap[teamId] = team;
     } catch (error) {
       console.error(`Failed to fetch team ${teamId}:`, error);
     }
   }));
   setTeams(teamsMap);
 };

 // Fetch matches with teams
 useEffect(() => {
   const fetchData = async () => {
     try {
       setLoading(true);
       const dateStr = selectedDate.toISOString().split('T')[0];
       const matchesData = await api.matches.getAll(dateStr);
       setMatches(matchesData);
       await fetchTeams(matchesData);
     } catch (err) {
       setError(err.message);
     } finally {
       setLoading(false);
     }
   };

   fetchData();
 }, [selectedDate]);

 // Get team name helper
 const getTeamName = (teamId) => {
   const team = teams[teamId];
   if (!team) return 'Loading...';
   
   // Remove (M) or (F) from the end of the name
   return team.name.replace(/\s*\([MF]\)$/, '');
 };

 return (
   <div className="py-4 space-y-4">
     {/* Date Selector */}
     <div className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
       <div className="flex items-center gap-2 mb-2">
         <Calendar className="w-5 h-5 text-primary-500" />
         <input
           type="date"
           value={selectedDate.toISOString().split('T')[0]}
           onChange={(e) => setSelectedDate(new Date(e.target.value))}
           className="bg-transparent border border-gray-200 dark:border-dark-border rounded px-2 py-1
                    text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
         />
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
       ) : matches.length === 0 ? (
         <div className="bg-white dark:bg-dark-card rounded-lg p-8 text-center text-gray-500 dark:text-dark-text-dim">
           No matches scheduled for this date
         </div>
       ) : (
        matches.map(match => (
          <div key={match.id} 
              className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg
                        hover:shadow-xl transition-shadow cursor-pointer">
            <div className="flex flex-col">
              {/* Teams and Time Row */}
              <div className="flex justify-center items-center gap-8">
                {/* Home Team */}
                <div className="flex items-center gap-2 w-1/3 justify-end">
                  <span className="font-medium text-gray-900 dark:text-dark-text text-right">
                    {getTeamName(match.home_team_id)}
                  </span>
                  <TeamLogo teamId={match.home_team_id} />
                </div>
        
                {/* Time - Split into two lines and fixed colors */}
                <div className="flex flex-col items-center w-1/3">
                  <span className="text-lg font-medium text-gray-900 dark:text-dark-text">
                    {new Date(match.scheduled_time).toLocaleTimeString('en-US', {
                      hour: '2-digit',
                      minute: '2-digit',
                      hour12: true
                    })}
                  </span>
                  <span className="text-sm text-gray-500 dark:text-gray-400">
                  {formatMatchTime(match.scheduled_time, match.timezone).split(' ').pop()}
                  </span>
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
                  <span className="font-medium text-gray-900 dark:text-dark-text">
                    {getTeamName(match.away_team_id)}
                  </span>
                </div>
              </div>
        
              {/* Conference Match Tag */}
              {match.is_conference_match && (
                <div className="flex justify-center mt-3">
                  <span className="text-xs bg-primary-100 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 px-2 py-0.5 rounded-full inline-flex items-center">
                    Conference Match
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