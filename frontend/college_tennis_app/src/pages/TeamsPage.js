import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Search, User } from "lucide-react";
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

const TeamsPage = () => {
  const [teams, setTeams] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  // Fetch all teams once on component mount
  useEffect(() => {
    const fetchTeams = async () => {
      try {
        setLoading(true);
        const teamsData = await api.teams.getAll();
        setTeams(teamsData);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTeams();
  }, []);

  // Filter teams based on search query and Division 1 only
  const filteredTeams = teams.filter((team) => {
    const searchLower = searchQuery.toLowerCase();
    return (
      team.division === "DIV_I" &&
      (team.name.toLowerCase().includes(searchLower) ||
        team.conference?.toLowerCase().includes(searchLower) ||
        team.abbreviation?.toLowerCase().includes(searchLower))
    );
  });

  // Format gender for display
  const formatTeamType = (team) => {
    return team.gender === "MALE" ? "Men's" : "Women's";
  };

  return (
    <div className="max-w-3xl mx-auto py-4 px-4">
      {/* Search Section */}
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-6 mb-6">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
          <input
            type="text"
            placeholder="Search for a team..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-3 border dark:border-dark-border rounded-lg
                    bg-transparent focus:ring-2 focus:ring-primary-500 text-lg
                    text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400"
            autoFocus
          />
        </div>
      </div>

      {/* Results Section */}
      <div className="space-y-4">
        {loading ? (
          // Loading state
          Array(3)
            .fill(0)
            .map((_, i) => (
              <div
                key={i}
                className="animate-pulse bg-white dark:bg-dark-card rounded-lg p-4"
              >
                <div className="flex items-center gap-4">
                  <div className="w-16 h-16 bg-gray-200 dark:bg-gray-700 rounded-lg"></div>
                  <div className="flex-1 space-y-2">
                    <div className="h-4 bg-gray-200 dark:bg-gray-700 rounded w-3/4"></div>
                    <div className="h-3 bg-gray-200 dark:bg-gray-700 rounded w-1/2"></div>
                  </div>
                </div>
              </div>
            ))
        ) : error ? (
          // Error state
          <div className="bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 p-4 rounded-lg">
            {error}
          </div>
        ) : searchQuery && filteredTeams.length === 0 ? (
          // No results state
          <div className="bg-white dark:bg-dark-card rounded-lg p-8 text-center">
            <div className="text-gray-500 dark:text-gray-400">
              No teams found matching "{searchQuery}"
            </div>
          </div>
        ) : searchQuery ? (
          // Results list
          filteredTeams.map((team) => (
            <div
              key={team.id}
              onClick={() => navigate(`/teams/${team.id}`)}
              className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-4 cursor-pointer
                      hover:shadow-xl transition-shadow"
            >
              <div className="flex items-center gap-4">
                <TeamLogo teamId={team.id} />
                <div className="flex-1">
                  <h3 className="font-semibold text-lg text-gray-900 dark:text-dark-text">
                    {team.name}
                  </h3>
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    {team.conference?.replace(/_/g, " ")}
                  </div>
                </div>
              </div>
            </div>
          ))
        ) : (
          // Initial state (no search yet)
          <div className="bg-white dark:bg-dark-card rounded-lg p-8 text-center">
            <div className="text-gray-500 dark:text-gray-400">
              Search for a team by name, conference, or abbreviation
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default TeamsPage;
