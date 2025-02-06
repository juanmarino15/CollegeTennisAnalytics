import React, { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { User, Calendar, ChevronRight, ChevronLeft } from "lucide-react";
import { api } from "../services/api";

// TeamLogo component (reused from other pages)
const TeamLogo = ({ teamId, size = "w-16 h-16", mobileSize = "w-12 h-12" }) => {
  const [hasError, setHasError] = useState(false);

  if (!teamId || hasError) {
    return (
      <div
        className={`${size} ${mobileSize} flex items-center justify-center text-gray-400 dark:text-gray-600`}
      >
        <span className="text-xs">Logo</span>
      </div>
    );
  }

  return (
    <div
      className={`${size} ${mobileSize} flex items-center justify-center mix-blend-multiply dark:mix-blend-normal`}
    >
      <img
        src={`http://localhost:8000/api/v1/teams/${teamId}/logo`}
        alt="Team Logo"
        className="object-contain mix-blend-multiply dark:mix-blend-normal"
        onError={() => setHasError(true)}
      />
    </div>
  );
};

const TeamDetailsPage = () => {
  const { teamId } = useParams();
  const navigate = useNavigate();
  const [team, setTeam] = useState(null);
  const [roster, setRoster] = useState([]);
  const [matches, setMatches] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const abortController = new AbortController();

    const fetchTeamData = async () => {
      try {
        setLoading(true);
        const [teamData, rosterData, matchesData, statsData] =
          await Promise.all([
            api.teams.getById(teamId, abortController.signal),
            api.teams.getRoster(teamId, abortController.signal),
            api.matches.getByTeam(teamId, abortController.signal),
            api.stats.getTeamStats(teamId, abortController.signal),
          ]);

        setTeam(teamData);
        console.log(teamData);
        setRoster(rosterData);
        console.log(rosterData);
        setMatches(matchesData);
        console.log(matchesData);
        setStats(statsData);
        console.log(statsData);
      } catch (err) {
        if (err.name === "AbortError") {
          return;
        }
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTeamData();

    return () => {
      abortController.abort();
    };
  }, [teamId]);

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto py-4 px-4">
        <div className="animate-pulse space-y-6">
          <div className="h-40 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
          <div className="h-60 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-7xl mx-auto py-4 px-4">
        <div className="bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 p-4 rounded-lg">
          {error}
        </div>
      </div>
    );
  }

  if (!team) return null;

  return (
    <div className="max-w-7xl mx-auto py-4 px-4 space-y-6">
      {/* Back Button */}
      <button
        onClick={() => navigate("/teams")}
        className="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200"
      >
        <ChevronLeft className="w-5 h-5" />
        Back to Teams
      </button>

      {/* Team Header */}
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
        <div className="flex flex-col sm:flex-row items-center gap-6">
          <TeamLogo teamId={team.id} size="w-24 h-24" />
          <div className="text-center sm:text-left">
            <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-dark-text">
              {team.name}
            </h1>
            <div className="text-gray-500 dark:text-gray-400 mt-1">
              {team.conference?.replace(/_/g, " ")}
            </div>
            <div className="text-gray-500 dark:text-gray-400">
              {team.gender === "MALE" ? "Men's" : "Women's"} Tennis
            </div>
          </div>
        </div>

        {/* Team Stats */}
        {stats && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-8">
            <div className="text-center p-4 bg-gray-50 dark:bg-dark-card-light rounded-lg">
              <div className="text-2xl font-bold text-gray-900 dark:text-dark-text">
                {stats.total_wins}-{stats.total_losses}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                Overall
              </div>
            </div>
            <div className="text-center p-4 bg-gray-50 dark:bg-dark-card-light rounded-lg">
              <div className="text-2xl font-bold text-gray-900 dark:text-dark-text">
                {stats.conference_wins}-{stats.conference_losses}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                Conference
              </div>
            </div>
            <div className="text-center p-4 bg-gray-50 dark:bg-dark-card-light rounded-lg">
              <div className="text-2xl font-bold text-gray-900 dark:text-dark-text">
                {stats.home_wins}-{stats.home_losses}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                Home
              </div>
            </div>
            <div className="text-center p-4 bg-gray-50 dark:bg-dark-card-light rounded-lg">
              <div className="text-2xl font-bold text-gray-900 dark:text-dark-text">
                {stats.away_wins}-{stats.away_losses}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                Away
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Roster Section */}
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
        <h2 className="text-xl font-bold mb-6 text-gray-900 dark:text-dark-text flex items-center gap-2">
          <User className="w-5 h-5" />
          Roster
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {roster.map((player) => (
            <div
              key={player.person_id}
              onClick={() => navigate(`/players/${player.person_id}`)}
              className="flex items-center gap-4 p-4 border dark:border-dark-border rounded-lg
                        hover:bg-gray-50 dark:hover:bg-dark-hover cursor-pointer"
            >
              {player.avatar_url ? (
                <img
                  src={player.avatar_url}
                  alt={`${player.first_name} ${player.last_name}`}
                  className="w-12 h-12 rounded-full object-cover"
                />
              ) : (
                <div className="w-12 h-12 rounded-full bg-gray-200 dark:bg-gray-700 flex items-center justify-center">
                  <User className="w-6 h-6 text-gray-400" />
                </div>
              )}
              <div>
                <div className="font-medium text-gray-900 dark:text-dark-text">
                  {player.first_name} {player.last_name}
                </div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {player.class_year}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Matches Section */}
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6">
        <h2 className="text-xl font-bold mb-6 text-gray-900 dark:text-dark-text flex items-center gap-2">
          <Calendar className="w-5 h-5" />
          Schedule & Results
        </h2>
        <div className="space-y-4">
          {matches.map((match) => (
            <div
              key={match.id}
              onClick={() => navigate(`/matches/${match.id}`)}
              className="flex justify-between items-center p-4 border dark:border-dark-border rounded-lg
                        hover:bg-gray-50 dark:hover:bg-dark-hover cursor-pointer"
            >
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  {match.is_conference_match && (
                    <span
                      className="text-xs px-2 py-0.5 bg-primary-100 dark:bg-primary-900/20 
                                   text-primary-700 dark:text-primary-300 rounded-full"
                    >
                      Conference
                    </span>
                  )}
                  <span className="font-medium text-gray-900 dark:text-dark-text">
                    {match.home_team_id === team.id ? "vs " : "@ "}
                    {match.home_team_id === team.id
                      ? match.away_team_name
                      : match.home_team_name}
                  </span>
                </div>
                <div className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                  {new Date(match.start_date).toLocaleDateString()} •
                  {match.completed
                    ? " Final"
                    : match.scheduled_time
                    ? ` ${new Date(
                        match.scheduled_time + "Z"
                      ).toLocaleTimeString("en-US", {
                        hour: "2-digit",
                        minute: "2-digit",
                        hour12: true,
                        timeZone: match.timezone,
                      })}`
                    : " TBA"}
                </div>
              </div>
              <ChevronRight className="w-5 h-5 text-gray-400" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default TeamDetailsPage;
