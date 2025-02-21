import React, { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { User, Calendar, ChevronRight, ChevronLeft } from "lucide-react";
import { api } from "../services/api";

const TeamLogo = ({ teamId, size = "w-20 h-20" }) => {
  const [hasError, setHasError] = useState(false);

  if (!teamId || hasError) {
    return (
      <div
        className={`${size} flex items-center justify-center text-gray-400 dark:text-gray-600`}
      >
        <span className="text-xs">Logo</span>
      </div>
    );
  }

  return (
    <div
      className={`${size} flex items-center justify-center mix-blend-multiply dark:mix-blend-normal`}
    >
      <img
        src={`http://localhost:8000/api/v1/teams/${teamId}/logo`}
        alt="Team Logo"
        className="w-full h-full object-contain mix-blend-multiply dark:mix-blend-normal"
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
  const [selectedSeason, setSelectedSeason] = useState("2024");
  const [selectedDate, setSelectedDate] = useState(new Date());
  const [seasons] = useState(["2024", "2023", "2022", "2021"]);
  const [matchScores, setMatchScores] = useState({});

  const handleSeasonChange = (e) => {
    setSelectedSeason(e.target.value);
  };

  const handleDateChange = (e) => {
    setSelectedDate(new Date(e.target.value));
  };

  useEffect(() => {
    const abortController = new AbortController();

    const fetchTeamData = async () => {
      try {
        setLoading(true);
        const dateStr = selectedDate.toISOString().split("T")[0];
        const [teamData, rosterData, matchesData, statsData, allMatches] =
          await Promise.all([
            api.teams.getById(teamId, abortController.signal),
            api.teams.getRoster(teamId, selectedSeason, abortController.signal),
            api.matches.getAllByTeam(
              teamId,
              selectedSeason,
              abortController.signal
            ),
            api.stats.getTeamStats(
              teamId,
              selectedSeason,
              abortController.signal
            ),
          ]);

        // Get scores for completed matches
        const completedMatches = matchesData.filter((match) => match.completed);
        const scorePromises = completedMatches.map((match) =>
          api.matches.getScore(match.id, abortController.signal)
        );
        const scores = await Promise.all(scorePromises);

        // Create scores map
        const scoresMap = {};
        completedMatches.forEach((match, index) => {
          scoresMap[match.id] = scores[index];
        });

        setTeam(teamData);
        setRoster(rosterData);
        setMatches(matchesData);
        setMatchScores(scoresMap);
        setStats(statsData);

        console.log(scoresMap);
      } catch (err) {
        if (err.name === "AbortError") return;
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTeamData();
    return () => abortController.abort();
  }, [teamId, selectedSeason, selectedDate]);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-dark-bg px-4 py-4">
        <div className="animate-pulse space-y-4">
          <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
          <div className="h-48 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-dark-bg px-4 py-4">
        <div className="bg-primary-50 dark:bg-primary-900/20 text-primary-600 dark:text-primary-400 p-4 rounded-lg">
          {error}
        </div>
      </div>
    );
  }

  if (!team) return null;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-dark-bg">
      {/* Back Button - Fixed position */}
      <div className="sticky top-0 z-10 bg-gray-50 dark:bg-dark-bg px-4 py-3 border-b border-gray-200 dark:border-dark-border">
        <button
          onClick={() => navigate("/teams")}
          className="flex items-center gap-2 text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300"
        >
          <ChevronLeft className="w-5 h-5" />
          <span>Back to Teams</span>
        </button>
      </div>

      <div className="p-4 space-y-4">
        {/* Team Header */}
        <div className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
          <div className="flex flex-col items-center gap-4">
            <TeamLogo teamId={team.id} />
            <div className="text-center">
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-text">
                {team.name}
              </h1>
              <div className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                {team.conference?.replace(/_/g, " ")}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                {team.gender === "MALE" ? "Men's" : "Women's"} Tennis
              </div>
            </div>
          </div>

          {/* Season Selector */}
          <div className="flex justify-center mt-4">
            <div className="inline-flex items-center gap-2 bg-gray-50 dark:bg-dark-card/50 px-3 py-2 rounded-lg border border-gray-200 dark:border-dark-border">
              <Calendar className="w-4 h-4 text-gray-500 dark:text-gray-400" />
              <select
                value={selectedSeason}
                onChange={handleSeasonChange}
                className="bg-transparent text-sm text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500
                         appearance-none cursor-pointer focus:outline-none"
              >
                {seasons.map((season) => (
                  <option key={season} value={season}>
                    {season}-{parseInt(season) + 1} Season
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Team Stats - Grid layout for mobile */}
          {stats && (
            <div className="grid grid-cols-2 gap-3 mt-6">
              <div className="text-center p-3 bg-gray-50 dark:bg-dark-card rounded-lg shadow border border-gray-200 dark:border-dark-border">
                <div className="text-xl font-bold text-gray-900 dark:text-dark-text">
                  {stats.total_wins}-{stats.total_losses}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  Overall
                </div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-dark-card rounded-lg shadow border border-gray-200 dark:border-dark-border">
                <div className="text-xl font-bold text-gray-900 dark:text-dark-text">
                  {stats.conference_wins}-{stats.conference_losses}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  Conference
                </div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-dark-card rounded-lg shadow border border-gray-200 dark:border-dark-border">
                <div className="text-xl font-bold text-gray-900 dark:text-dark-text">
                  {stats.home_wins}-{stats.home_losses}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  Home
                </div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-dark-card rounded-lg shadow border border-gray-200 dark:border-dark-border">
                <div className="text-xl font-bold text-gray-900 dark:text-dark-text">
                  {stats.away_wins}-{stats.away_losses}
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  Away
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Roster Section */}
        <div className="bg-white dark:bg-dark-card rounded-lg p-3 shadow-lg">
          <h2 className="text-base font-bold mb-4 text-gray-900 dark:text-dark-text flex items-center gap-2">
            <User className="w-4 h-4" />
            Roster
          </h2>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {roster.map((player) => (
              <div
                key={player.person_id}
                onClick={() => navigate(`/players/${player.person_id}`)}
                className="flex items-center gap-3 py-2 px-3 bg-gray-50 dark:bg-dark-card rounded-lg shadow-sm
                          border border-gray-200 dark:border-dark-border active:bg-gray-100 
                          dark:active:bg-gray-700/50 transition-colors"
              >
                {player.avatar_url ? (
                  <img
                    src={player.avatar_url}
                    alt={`${player.first_name} ${player.last_name}`}
                    className="w-8 h-8 rounded-full object-cover flex-shrink-0"
                  />
                ) : (
                  <div className="w-5 h-5 rounded-full bg-gray-200 dark:bg-gray-700 flex items-center justify-center flex-shrink-0">
                    <User className="w-3 h-3 text-gray-400" />
                  </div>
                )}
                <div className="flex-1 min-w-0">
                  <div className="font-medium text-sm text-gray-900 dark:text-dark-text">
                    {player.first_name} {player.last_name}
                  </div>
                </div>
                <ChevronRight className="w-4 h-4 text-gray-400 flex-shrink-0" />
              </div>
            ))}
          </div>
        </div>

        {/* Matches Section */}
        <div className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
          <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 mb-4">
            <h2 className="text-lg font-bold text-gray-900 dark:text-dark-text flex items-center gap-2">
              <Calendar className="w-5 h-5" />
              Schedule & Results
            </h2>
          </div>

          <div className="space-y-2">
            {matches.length === 0 ? (
              <div className="text-center py-4 text-gray-500 dark:text-gray-400 text-sm">
                No matches scheduled
              </div>
            ) : (
              matches.map((match) => (
                <div
                  key={match.id}
                  onClick={() => navigate(`/matches/${match.id}`)}
                  className="bg-white dark:bg-dark-card rounded-lg shadow border border-gray-200 
                dark:border-dark-border hover:shadow-md transition-shadow cursor-pointer"
                >
                  <div className="p-3">
                    <div className="relative">
                      {/* Result/Time - Absolute positioned */}
                      <div
                        className="absolute top-0 "
                        style={{ right: "-4px", top: "-10px" }}
                      >
                        {match.completed ? (
                          <div
                            className={`inline-block px-1 py-0.5 rounded border
          ${
            matchScores[match.id] &&
            ((match.home_team_id === team.id &&
              matchScores[match.id].home_team_won) ||
              (match.away_team_id === team.id &&
                matchScores[match.id].away_team_won))
              ? "border-green-500 text-green-600"
              : "border-red-500 text-red-600"
          } text-[9px] font-medium`}
                          >
                            {matchScores[match.id]
                              ? match.home_team_id === team.id
                                ? matchScores[match.id].home_team_won
                                  ? `W, ${
                                      matchScores[match.id].home_team_score
                                    }-${matchScores[match.id].away_team_score}`
                                  : `L, ${
                                      matchScores[match.id].away_team_score
                                    }-${matchScores[match.id].home_team_score}`
                                : matchScores[match.id].away_team_won
                                ? `W, ${
                                    matchScores[match.id].away_team_score
                                  }-${matchScores[match.id].home_team_score}`
                                : `L, ${
                                    matchScores[match.id].home_team_score
                                  }-${matchScores[match.id].away_team_score}`
                              : "Loading..."}
                          </div>
                        ) : (
                          <div
                            className="text-[9px] font-medium text-gray-900 dark:text-dark-text"
                            style={{ right: "-4px" }}
                          >
                            {match.scheduled_time
                              ? new Date(
                                  match.scheduled_time + "Z"
                                ).toLocaleTimeString("en-US", {
                                  hour: "numeric",
                                  minute: "2-digit",
                                  hour12: true,
                                })
                              : "TBA"}
                          </div>
                        )}
                      </div>

                      {/* Main Content Grid */}
                      <div className="grid grid-cols-12 items-center gap-2">
                        {/* Left Side - Game Info */}
                        <div className="col-span-3">
                          <div className="text-[9px] uppercase text-gray-500 dark:text-gray-400">
                            {match.home_team_id === team.id ? "HOME" : "AWAY"}
                          </div>
                          <div className="text-xs font-bold text-gray-900 dark:text-dark-text">
                            {new Date(match.start_date)
                              .toLocaleDateString("en-US", {
                                weekday: "short",
                              })
                              .toUpperCase()}
                            .
                          </div>
                          <div className="text-xs font-bold text-gray-900 dark:text-dark-text">
                            JAN {new Date(match.start_date).getDate()}
                          </div>
                        </div>

                        {/* Team Logo */}
                        <div
                          className="flex justify-center col-span-2"
                          style={{ justifyContent: "end" }}
                        >
                          <TeamLogo
                            teamId={
                              match.home_team_id === team.id
                                ? match.away_team_id
                                : match.home_team_id
                            }
                            size="w-8 h-8"
                          />
                        </div>

                        {/* Team Name */}
                        <div className="col-span-7 pr-8">
                          <div className="text-[9px] font-medium text-gray-900 dark:text-dark-text">
                            {match.home_team_id === team.id
                              ? match.away_team_name
                              : match.home_team_name}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default TeamDetailsPage;
