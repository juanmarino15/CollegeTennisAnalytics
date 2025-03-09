import React, { useState, useEffect, useMemo } from "react";
import { Calendar, Clock, ChevronDown, ArrowUpDown } from "lucide-react";
import { api } from "../services/api";
import { useNavigate } from "react-router-dom";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

const STORAGE_KEY_DATE = "tennis_app_selected_date";
const STORAGE_KEY_FILTERS = "tennis_app_filters";

// TeamLogo component with responsive sizing
const TeamLogo = ({ teamId }) => {
  const [hasError, setHasError] = useState(false);

  if (!teamId || hasError) {
    return (
      <div className="w-14 h-14 sm:w-14 sm:h-14 md:w-12 md:h-12 flex items-center justify-center text-gray-400 dark:text-gray-600">
        <span className="text-xs">Logo</span>
      </div>
    );
  }

  return (
    <div className="w-8 h-8 sm:w-14 sm:h-14 md:w-12 md:h-12 flex items-center justify-center mix-blend-multiply dark:mix-blend-normal">
      <img
        src={`http://localhost:8000/api/v1/teams/${teamId}/logo`}
        alt="Team Logo"
        className="w-full h-full object-contain mix-blend-multiply dark:mix-blend-normal"
        onError={() => setHasError(true)}
      />
    </div>
  );
};

//get initial state
const getInitialDate = () => {
  const storedDate = sessionStorage.getItem(STORAGE_KEY_DATE);
  return storedDate ? new Date(storedDate) : new Date();
};

//get initial filters
const getInitialFilters = () => {
  const storedFilters = sessionStorage.getItem(STORAGE_KEY_FILTERS);
  return storedFilters
    ? JSON.parse(storedFilters)
    : {
        gender: "",
        conference: "",
        sort: "time-asc",
      };
};

const MatchesPage = () => {
  const [showFilters, setShowFilters] = useState(false);
  const [matches, setMatches] = useState([]);
  const [teams, setTeams] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedDate, setSelectedDate] = useState(getInitialDate());
  const [matchScores, setMatchScores] = useState({}); //match scores
  const [availableConferences, setAvailableConferences] = useState(new Set());
  const navigate = useNavigate();
  const [filters, setFilters] = useState(getInitialFilters());

  const updateDate = (date) => {
    setSelectedDate(date);
    sessionStorage.setItem(STORAGE_KEY_DATE, date.toISOString());
  };

  const updateFilters = (newValues) => {
    const updatedFilters = { ...filters, ...newValues };
    setFilters(updatedFilters);
    sessionStorage.setItem(STORAGE_KEY_FILTERS, JSON.stringify(updatedFilters));
  };

  // Sort matches by time
  const sortedMatches = useMemo(() => {
    return [...matches].sort((a, b) => {
      // First handle TBD matches - push them to the end
      const aIsTBD =
        !a.scheduled_time ||
        new Date(a.scheduled_time + "Z").toString() === "Invalid Date";
      const bIsTBD =
        !b.scheduled_time ||
        new Date(b.scheduled_time + "Z").toString() === "Invalid Date";

      if (aIsTBD && !bIsTBD) return 1; // a goes after b
      if (!aIsTBD && bIsTBD) return -1; // a goes before b
      if (aIsTBD && bIsTBD) return 0; // keep original order for TBD vs TBD

      // If neither is TBD, proceed with normal sorting
      const timeA = new Date(a.scheduled_time + "Z");
      const timeB = new Date(b.scheduled_time + "Z");

      switch (filters.sort) {
        case "time-desc":
          return timeB - timeA;
        case "conference":
          // Sort conference matches first, then by time
          if (a.is_conference_match && !b.is_conference_match) return -1;
          if (!a.is_conference_match && b.is_conference_match) return 1;
          return timeA - timeB;
        case "completed":
          // Sort completed matches first, then by time
          if (a.completed && !b.completed) return -1;
          if (!a.completed && b.completed) return 1;
          return timeA - timeB;
        case "time-asc":
        default:
          return timeA - timeB;
      }
    });
  }, [matches, filters.sort]);

  // Helper function to format conference name
  const formatConferenceName = (conference) => {
    return conference.replace(/_/g, " ");
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

  // Fetch team data
  const fetchTeams = async (matchesData) => {
    const teamIds = new Set();
    const conferences = new Set(); // Add this

    matchesData.forEach((match) => {
      teamIds.add(match.home_team_id);
      teamIds.add(match.away_team_id);
    });

    const teamsMap = {};
    await Promise.all(
      [...teamIds].map(async (teamId) => {
        try {
          const team = await api.teams.getById(teamId);
          teamsMap[teamId] = team;
          if (team.conference) {
            // Add this
            conferences.add(team.conference);
          }
        } catch (error) {
          console.error(`Failed to fetch team ${teamId}:`, error);
        }
      })
    );

    setAvailableConferences(conferences); // Add this
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

        const dateStr = selectedDate.toISOString().split("T")[0];
        let matchesData = await api.matches.getAll(
          dateStr,
          abortController.signal
        );

        if (!isMounted) return;

        // First get all team data
        const teamIds = new Set();
        matchesData.forEach((match) => {
          teamIds.add(match.home_team_id);
          teamIds.add(match.away_team_id);
        });

        // Fetch all team data in parallel
        const teamPromises = Array.from(teamIds).map((teamId) =>
          api.teams.getById(teamId, abortController.signal)
        );
        const teamResults = await Promise.all(teamPromises);

        if (!isMounted) return;

        // Create teams map
        const teamsMap = {};
        const conferences = new Set();
        teamResults.forEach((team) => {
          teamsMap[team.id] = team;
          if (team.conference) {
            conferences.add(team.conference);
          }
        });

        // Apply filters
        let filteredMatches = matchesData;
        if (filters.gender) {
          filteredMatches = filteredMatches.filter(
            (match) => match.gender === filters.gender
          );
        }
        if (filters.conference) {
          filteredMatches = filteredMatches.filter((match) => {
            const homeTeam = teamsMap[match.home_team_id];
            const awayTeam = teamsMap[match.away_team_id];
            return (
              homeTeam?.conference === filters.conference ||
              awayTeam?.conference === filters.conference
            );
          });
        }

        // Fetch scores for completed matches in parallel
        const completedMatches = filteredMatches.filter(
          (match) => match.completed
        );
        const scorePromises = completedMatches.map((match) =>
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
        if (err.name === "AbortError") return;
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
    if (!team) return "Loading...";

    // Remove (M) or (F) from the end of the name
    return team.name;
  };

  return (
    <div className="py-4 space-y-4">
      {/* Date and Filters Section */}
      <div className="bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg">
        <div className="flex flex-col gap-4">
          {/* Date selector - Always visible */}
          <div className="w-full">
            <div className="flex items-center gap-2">
              <Calendar className="w-5 h-5 text-primary-500 flex-shrink-0" />
              <DatePicker
                selected={selectedDate}
                onChange={(date) => updateDate(date)}
                className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2
                text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
                dateFormat="yyyy-MM-dd"
                placeholderText="Select a date"
              />
            </div>
          </div>

          {/* Collapsible Filters Section */}
          <div className="sm:hidden">
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="w-full flex items-center justify-between py-2 px-3 border border-gray-200 dark:border-dark-border rounded text-sm font-medium text-gray-700 dark:text-gray-300"
            >
              <span>Filters & Sort</span>
              <ChevronDown
                className={`w-5 h-5 transition-transform ${
                  showFilters ? "rotate-180" : ""
                }`}
              />
            </button>

            {showFilters && (
              <div className="mt-3 p-3 border border-gray-200 dark:border-dark-border rounded bg-white dark:bg-dark-card animate-fade-in space-y-3">
                {/* Gender Filter */}
                <div className="flex flex-col gap-1">
                  <label className="text-sm text-gray-600 dark:text-gray-400">
                    Gender:
                  </label>
                  <select
                    value={filters.gender}
                    onChange={(e) => updateFilters({ gender: e.target.value })}
                    className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
                  >
                    <option value="">All</option>
                    <option value="MALE">Men</option>
                    <option value="FEMALE">Women</option>
                  </select>
                </div>

                {/* Conference Filter */}
                <div className="flex flex-col gap-1">
                  <label className="text-sm text-gray-600 dark:text-gray-400">
                    Conference:
                  </label>
                  <select
                    value={filters.conference}
                    onChange={(e) =>
                      updateFilters({ conference: e.target.value })
                    }
                    className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
                  >
                    <option value="">All Matches</option>
                    {[...availableConferences].sort().map((conf) => (
                      <option key={conf} value={conf}>
                        {formatConferenceName(conf)}
                      </option>
                    ))}
                  </select>
                </div>

                {/* Sort Filter */}
                <div className="flex flex-col gap-1">
                  <label className="text-sm text-gray-600 dark:text-gray-400 flex items-center">
                    <ArrowUpDown className="w-4 h-4 mr-1" />
                    Sort by:
                  </label>
                  <select
                    value={filters.sort}
                    onChange={(e) => updateFilters({ sort: e.target.value })}
                    className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
                  >
                    <option value="time-asc">Start Time (Earliest)</option>
                    <option value="time-desc">Start Time (Latest)</option>
                    <option value="conference">Conference First</option>
                    <option value="completed">Completed First</option>
                  </select>
                </div>
              </div>
            )}
          </div>

          {/* Regular Layout for Larger Screens */}
          <div className="hidden sm:flex sm:flex-row sm:flex-wrap sm:gap-4">
            {/* Gender Filter */}
            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                Gender:
              </label>
              <select
                value={filters.gender}
                onChange={(e) =>
                  setFilters((prev) => ({ ...prev, gender: e.target.value }))
                }
                className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
              >
                <option value="">All</option>
                <option value="MALE">Men</option>
                <option value="FEMALE">Women</option>
              </select>
            </div>

            {/* Conference Filter */}
            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                Conference:
              </label>
              <select
                value={filters.conference}
                onChange={(e) =>
                  setFilters((prev) => ({
                    ...prev,
                    conference: e.target.value,
                  }))
                }
                className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
              >
                <option value="">All Matches</option>
                {[...availableConferences].sort().map((conf) => (
                  <option key={conf} value={conf}>
                    {formatConferenceName(conf)}
                  </option>
                ))}
              </select>
            </div>

            {/* Sort Filter */}
            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap flex items-center">
                <ArrowUpDown className="w-4 h-4 mr-1" />
                Sort:
              </label>
              <select
                value={filters.sort}
                onChange={(e) =>
                  setFilters((prev) => ({ ...prev, sort: e.target.value }))
                }
                className="w-full bg-transparent border border-gray-200 dark:border-dark-border rounded px-3 py-2 text-gray-900 dark:text-dark-text focus:ring-2 focus:ring-primary-500"
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
            <div
              key={i}
              className="animate-pulse bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg"
            >
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
          sortedMatches.map((match) => (
            <div
              key={match.id}
              onClick={() => navigate(`/matches/${match.id}`)}
              className={`relative bg-white dark:bg-dark-card rounded-lg p-4 shadow-lg
                    hover:shadow-xl transition-shadow cursor-pointer
                    ${
                      match.is_conference_match
                        ? "border-l-4 border-primary-500 dark:border-primary-400"
                        : ""
                    }`}
            >
              <div className="flex flex-col">
                {/* Teams and Time Row */}
                <div className="flex justify-center items-center gap-4 sm:gap-8">
                  {/* Home Team */}
                  <div className="flex flex-col sm:flex-row items-center w-1/3 justify-end text-center sm:flex-row-reverse sm:justify-start sm:text-left">
                    <TeamLogo
                      teamId={match.home_team_id}
                      className="w-10 h-10 sm:w-8 sm:h-8"
                    />
                    <span
                      className={`text-[9px] sm:text-sm text-gray-900 dark:text-dark-text
      ${match.is_conference_match ? "font-semibold" : ""}`}
                    >
                      {getTeamName(match.home_team_id)}
                    </span>
                  </div>

                  {/* Time/Score Section */}
                  <div className="flex flex-col items-center w-1/3">
                    {match.completed ? (
                      <span className="text-sm sm:text-base font-medium text-gray-900 dark:text-dark-text">
                        {matchScores[match.id]
                          ? `${matchScores[match.id].home_team_score} - ${
                              matchScores[match.id].away_team_score
                            }`
                          : "vs"}
                      </span>
                    ) : (
                      <>
                        <span className="text-xs sm:text-sm font-medium text-gray-900 dark:text-dark-text">
                          {(() => {
                            try {
                              if (!match.scheduled_time) return "TBD";
                              const date = new Date(match.scheduled_time + "Z");
                              if (isNaN(date.getTime())) return "TBD";
                              return new Date(
                                match.scheduled_time + "Z"
                              ).toLocaleTimeString("en-US", {
                                hour: "2-digit",
                                minute: "2-digit",
                                hour12: true,
                                timeZone: match.timezone,
                              });
                            } catch (e) {
                              return "TBD";
                            }
                          })()}
                        </span>
                        <span className="text-xs sm:text-xs text-gray-500 dark:text-gray-400">
                          {(() => {
                            try {
                              if (!match.scheduled_time) return "";
                              const date = new Date(match.scheduled_time + "Z");
                              if (isNaN(date.getTime())) return "";
                              return formatMatchTime(
                                match.scheduled_time,
                                match.timezone
                              )
                                .split(" ")
                                .pop();
                            } catch (e) {
                              return "";
                            }
                          })()}
                        </span>
                      </>
                    )}
                    {match.completed && (
                      <span
                        className="text-[12px] leading-none px-1.5 py-0.5 mt-1 rounded-full inline-flex items-center
        bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400"
                      >
                        Final
                      </span>
                    )}
                  </div>

                  {/* Away Team */}
                  <div className="flex flex-col sm:flex-row items-center w-1/3 justify-start text-center sm:flex-row sm:justify-start sm:text-left">
                    <TeamLogo teamId={match.away_team_id} />
                    <span
                      className={`text-[9px] sm:text-sm text-gray-900 dark:text-dark-text 
      ${match.is_conference_match ? "font-semibold" : ""}`}
                    >
                      {getTeamName(match.away_team_id)}
                    </span>
                  </div>
                </div>

                {/* Conference Match Tag */}
                {match.is_conference_match && (
                  <div className="absolute top-1.5 left-1/2 -translate-x-1/2 sm:left-auto sm:right-2 sm:translate-x-0">
                    <span className="text-[10px] leading-none bg-primary-100/80 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 px-1.5 py-0.5 rounded-full inline-flex items-center">
                      Conference
                    </span>
                  </div>
                )}
              </div>
            </div>
          ))
        )}
      </div>{" "}
    </div>
  );
};

export default MatchesPage;
