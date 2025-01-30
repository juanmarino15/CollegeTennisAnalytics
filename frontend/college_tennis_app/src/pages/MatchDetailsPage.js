import React, { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { Calendar, MapPin, User, Clock, Check } from "lucide-react";
import { api } from "../services/api";

// Reuse TeamLogo component for consistency
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

// Helper function to format datetime
const formatMatchTime = (dateTimeStr, timezone) => {
  try {
    const date = new Date(dateTimeStr);
    return date.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      timeZoneName: "short",
      timeZone: timezone,
    });
  } catch (e) {
    return "Time TBD";
  }
};

// Helper function to format player name
const formatPlayerName = (player) => {
  if (!player) return "";
  return `${player.first_name.charAt(0)}. ${player.last_name}`;
};

const MatchDetailsPage = () => {
  const { matchId } = useParams();
  const [match, setMatch] = useState(null);
  const [lineup, setLineup] = useState([]);
  const [teams, setTeams] = useState({ home: null, away: null });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [matchScore, setMatchScore] = useState(null);
  const [players, setPlayers] = useState({});

  useEffect(() => {
    const abortController = new AbortController();
    let isMounted = true;

    const fetchMatchDetails = async () => {
      try {
        if (!isMounted) return;
        setLoading(true);

        // First fetch match data
        const matchData = await api.matches.getById(
          matchId,
          abortController.signal
        );
        if (!isMounted) return;

        // Then fetch teams, lineup, and score in parallel
        const [homeTeam, awayTeam, lineupData, scoreData] = await Promise.all([
          api.teams.getById(matchData.home_team_id, abortController.signal),
          api.teams.getById(matchData.away_team_id, abortController.signal),
          matchData.completed
            ? api.matches.getLineup(matchId, abortController.signal)
            : Promise.resolve([]),
          matchData.completed
            ? api.matches.getScore(matchId, abortController.signal)
            : Promise.resolve(null),
        ]);

        if (!isMounted) return;

        // Get unique player IDs from lineup
        const playerIds = new Set();
        lineupData.forEach((match) => {
          if (match.side1_player1_id) playerIds.add(match.side1_player1_id);
          if (match.side1_player2_id) playerIds.add(match.side1_player2_id);
          if (match.side2_player1_id) playerIds.add(match.side2_player1_id);
          if (match.side2_player2_id) playerIds.add(match.side2_player2_id);
        });

        // Fetch all player details in parallel
        const playerPromises = Array.from(playerIds).map((playerId) =>
          api.players.getById(playerId, abortController.signal)
        );
        const playerResults = await Promise.all(playerPromises);

        // Create players map
        const playersMap = {};
        playerResults.forEach((player) => {
          playersMap[player.person_id] = player;
        });

        setMatch(matchData);
        setTeams({ home: homeTeam, away: awayTeam });
        setLineup(lineupData);
        setMatchScore(scoreData);
        setPlayers(playersMap);
      } catch (err) {
        if (!isMounted) return;
        if (err.name === "AbortError") return;
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
  const doublesMatches = lineup
    .filter((match) => match.match_type === "DOUBLES")
    .sort((a, b) => a.position - b.position);
  const singlesMatches = lineup
    .filter((match) => match.match_type === "SINGLES")
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
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-4">
        <div className="flex flex-row justify-between items-center mb-4">
          {/* Home Team */}
          <div className="flex flex-col items-center flex-1">
            <TeamLogo
              teamId={match.home_team_id}
              size="w-16 h-16"
              mobileSize="w-12 h-12"
            />
            <div className="mt-2">
              <div
                className={`text-base md:text-lg font-medium text-gray-900 dark:text-dark-text text-center
                     ${match.is_conference_match ? "font-semibold" : ""}`}
              >
                {teams.home.name}
              </div>
              {teams.home.conference && (
                <div className="text-xs md:text-sm text-gray-500 dark:text-gray-400 text-center">
                  {teams.home.conference.replace(/_/g, " ")}
                </div>
              )}
            </div>
          </div>

          {/* Score/VS Section */}
          <div className="flex flex-col items-center mx-2">
            <span className="text-xl md:text-2xl font-bold text-gray-900 dark:text-dark-text">
              {match.completed && matchScore
                ? `${matchScore.home_team_score} - ${matchScore.away_team_score}`
                : "vs"}
            </span>
            {match.completed && (
              <span
                className="text-xs px-2 py-0.5 mt-1 rounded-full inline-flex items-center
                   bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400"
              >
                Final
              </span>
            )}
            {!match.completed && match.scheduled_time && (
              <span className="text-xs md:text-sm text-gray-600 dark:text-gray-400 mt-1">
                {new Date(match.scheduled_time + "Z").toLocaleTimeString(
                  "en-US",
                  {
                    hour: "2-digit",
                    minute: "2-digit",
                    hour12: true,
                    timeZone: match.timezone,
                  }
                )}
              </span>
            )}
          </div>

          {/* Away Team */}
          <div className="flex flex-col items-center flex-1">
            <TeamLogo
              teamId={match.away_team_id}
              size="w-16 h-16"
              mobileSize="w-12 h-12"
            />
            <div className="mt-2">
              <div
                className={`text-base md:text-lg font-medium text-gray-900 dark:text-dark-text text-center
                     ${match.is_conference_match ? "font-semibold" : ""}`}
              >
                {teams.away.name}
              </div>
              {teams.away.conference && (
                <div className="text-xs md:text-sm text-gray-500 dark:text-gray-400 text-center">
                  {teams.away.conference.replace(/_/g, " ")}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Match Details Row */}
        <div className="flex flex-col md:flex-row justify-center gap-2 md:gap-6 text-gray-600 dark:text-gray-400 mt-4">
          <div className="flex items-center justify-center gap-2">
            <Calendar className="w-4 h-4 md:w-5 md:h-5" />
            <span className="text-xs md:text-sm">
              {new Date(match.start_date).toLocaleDateString("en-US", {
                weekday: "long",
                year: "numeric",
                month: "long",
                day: "numeric",
              })}
            </span>
          </div>
          {match.scheduled_time && (
            <div className="flex items-center justify-center gap-2">
              <Clock className="w-4 h-4 md:w-5 md:h-5" />
              <span className="text-xs md:text-sm">
                {new Date(match.scheduled_time + "Z").toLocaleTimeString(
                  "en-US",
                  {
                    hour: "2-digit",
                    minute: "2-digit",
                    hour12: true,
                    timeZone: match.timezone,
                  }
                )}{" "}
                {formatMatchTime(match.scheduled_time, match.timezone)
                  .split(" ")
                  .pop()}
              </span>
            </div>
          )}
        </div>

        {match.is_conference_match && (
          <div className="mt-4 text-center">
            <span
              className="bg-primary-100 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 
                           text-xs md:text-sm font-medium px-3 py-1 rounded-full"
            >
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
            <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-4">
              <h2 className="text-xl font-bold mb-4 text-gray-900 dark:text-dark-text">
                Doubles
              </h2>
              <div className="space-y-4">
                {doublesMatches.map((match) => (
                  <div
                    key={match.id}
                    className="border dark:border-dark-border rounded-lg p-4"
                  >
                    <div className="flex-1">
                      {/* Match Number with Badge Design */}
                      <div className="flex justify-between items-center mb-3">
                        <span className="text-sm font-semibold bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 px-3 py-1 rounded-full">
                          Match #{match.position}
                        </span>
                        {/* UF Tag - if match is unfinished */}
                        {!match.side1_won && !match.side2_won && (
                          <span
                            className="text-xs px-2 py-0.5 rounded-full 
                       bg-yellow-100 dark:bg-yellow-900/20 
                       text-yellow-700 dark:text-yellow-400"
                          >
                            UF
                          </span>
                        )}
                      </div>

                      {/* Home Team Players */}
                      <div
                        className={`text-sm flex justify-between items-center py-2
  ${
    match.side1_won
      ? "text-green-600 dark:text-green-400 font-semibold"
      : "text-gray-600 dark:text-gray-400"
  }`}
                      >
                        <div className="flex items-center gap-2">
                          <User className="w-4 h-4" />
                          <span>
                            {formatPlayerName(players[match.side1_player1_id])}{" "}
                            /{" "}
                            {formatPlayerName(players[match.side1_player2_id])}{" "}
                            [{teams.home.abbreviation}]
                          </span>
                          {match.side1_won && (
                            <Check className="w-4 h-4 text-green-600 dark:text-green-400" />
                          )}
                        </div>
                        <div className="font-medium">
                          {match.side1_score.split(" ")[0].split("-")[0]}
                        </div>
                      </div>

                      {/* Away Team Players */}
                      <div
                        className={`text-sm flex justify-between items-center py-2
  ${
    match.side2_won
      ? "text-green-600 dark:text-green-400 font-semibold"
      : "text-gray-600 dark:text-gray-400"
  }`}
                      >
                        <div className="flex items-center gap-2">
                          <User className="w-4 h-4" />
                          <span>
                            {formatPlayerName(players[match.side2_player1_id])}{" "}
                            /{" "}
                            {formatPlayerName(players[match.side2_player2_id])}{" "}
                            [{teams.away.abbreviation}]
                          </span>
                          {match.side2_won && (
                            <Check className="w-4 h-4 text-green-600 dark:text-green-400" />
                          )}
                        </div>
                        <div className="font-medium">
                          {
                            match.side1_score
                              .split(" ")[0]
                              .split("-")[1]
                              .split("(")[0]
                          }
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
            <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-4">
              <h2 className="text-xl font-bold mb-4 text-gray-900 dark:text-dark-text">
                Singles
              </h2>
              <div className="space-y-4">
                {singlesMatches.map((match) => (
                  <div
                    key={match.id}
                    className="border dark:border-dark-border rounded-lg p-4"
                  >
                    <div className="flex-1">
                      {/* Match Number with Badge Design */}
                      <div className="flex justify-between items-center mb-3">
                        <span className="text-sm font-semibold bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 px-3 py-1 rounded-full">
                          Match #{match.position}
                        </span>
                        {/* UF Tag - if match is unfinished */}
                        {!match.side1_won && !match.side2_won && (
                          <span
                            className="text-xs px-2 py-0.5 rounded-full 
                       bg-yellow-100 dark:bg-yellow-900/20 
                       text-yellow-700 dark:text-yellow-400"
                          >
                            UF
                          </span>
                        )}
                      </div>

                      {/* Home Player */}
                      <div
                        className={`text-sm flex justify-between items-center py-2
  ${
    match.side1_won
      ? "text-green-600 dark:text-green-400 font-semibold"
      : "text-gray-600 dark:text-gray-400"
  }`}
                      >
                        <div className="flex items-center gap-2">
                          <User className="w-4 h-4" />
                          <span>
                            {formatPlayerName(players[match.side1_player1_id])}{" "}
                            [{teams.home.abbreviation}]
                          </span>
                          {match.side1_won && (
                            <Check className="w-4 h-4 text-green-600 dark:text-green-400" />
                          )}
                        </div>
                        <div className="font-medium space-x-2">
                          {match.side1_score.split(" ").map((set, index) => (
                            <span key={index}>{set.split("-")[0]}</span>
                          ))}
                        </div>
                      </div>

                      {/* Away Player */}
                      <div
                        className={`text-sm flex justify-between items-center py-2
  ${
    match.side2_won
      ? "text-green-600 dark:text-green-400 font-semibold"
      : "text-gray-600 dark:text-gray-400"
  }`}
                      >
                        <div className="flex items-center gap-2">
                          <User className="w-4 h-4" />
                          <span>
                            {formatPlayerName(players[match.side2_player1_id])}{" "}
                            [{teams.away.abbreviation}]
                          </span>
                          {match.side2_won && (
                            <Check className="w-4 h-4 text-green-600 dark:text-green-400" />
                          )}
                        </div>
                        <div className="font-medium space-x-2">
                          {match.side1_score.split(" ").map((set, index) => (
                            <span key={index}>
                              {set.split("-")[1].split("(")[0]}
                            </span>
                          ))}
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
