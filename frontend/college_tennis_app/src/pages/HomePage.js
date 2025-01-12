import React, { useState, useEffect } from 'react';
import { Clock, ChevronRight } from 'lucide-react';

const HomePage = () => {
  const [matches, setMatches] = useState([]);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    // Simulated fetch - replace with your actual API call
    setTimeout(() => {
      setMatches([
        { id: 1, homeTeam: 'Stanford', awayTeam: 'UCLA', time: '2:00 PM PST' },
        { id: 2, homeTeam: 'USC', awayTeam: 'Cal', time: '3:30 PM PST' },
      ]);
      setLoading(false);
    }, 1000);
  }, []);

  return (
    <div className="space-y-4 py-4">
      <div className="bg-white dark:bg-dark-card rounded-lg shadow-lg p-4">
        <h2 className="text-lg font-semibold mb-3 text-gray-900 dark:text-dark-text">
          Today's Matches
        </h2>
        <div className="space-y-3">
          {loading ? (
            // Loading skeleton
            [...Array(2)].map((_, i) => (
              <div key={i} className="animate-pulse">
                <div className="h-16 bg-gray-200 dark:bg-dark-border rounded-lg"></div>
              </div>
            ))
          ) : (
            matches.map(match => (
              <div key={match.id} className="p-3 border border-gray-200 dark:border-dark-border rounded-lg">
                <div className="flex justify-between items-center">
                  <div>
                    <div className="font-medium text-gray-900 dark:text-dark-text">
                      {match.homeTeam} vs {match.awayTeam}
                    </div>
                    <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-dark-text-dim mt-1">
                      <Clock className="w-4 h-4" />
                      {match.time}
                    </div>
                  </div>
                  <ChevronRight className="w-5 h-5 text-gray-400 dark:text-dark-text-dim" />
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
};

export default HomePage;