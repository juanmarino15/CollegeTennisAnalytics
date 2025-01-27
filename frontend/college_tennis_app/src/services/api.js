const BASE_URL = 'http://localhost:8000/api/v1';

export const api = {
  // Matches endpoints
  matches: {
    getAll: async (date, signal) => {
      const params = date ? `?date=${date}` : '';
      const response = await fetch(`${BASE_URL}/matches${params}`, { signal });
      if (!response.ok) throw new Error('Failed to fetch matches');
      return response.json();
    },
    getById: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/matches/${id}`, { signal });
      if (!response.ok) throw new Error('Failed to fetch match');
      return response.json();
    },
    getLineup: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/matches/${id}/lineup`, { signal });
      if (!response.ok) throw new Error('Failed to fetch match lineup');
      return response.json();
    },
    getScore: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/matches/${id}/score`, { signal });
      if (!response.ok) throw new Error('Failed to fetch match score');
      return response.json();
    }
  },

  // Teams endpoints
  teams: {
    getAll: async (signal) => {
      const response = await fetch(`${BASE_URL}/teams`, { signal });
      if (!response.ok) throw new Error('Failed to fetch teams');
      return response.json();
    },
    getById: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/teams/${id}`, { signal });
      if (!response.ok) throw new Error('Failed to fetch team');
      return response.json();
    },
    getLogo: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/teams/${id}/logo`, { signal });
      if (!response.ok) throw new Error('Failed to fetch team logo');
      return response.blob();
    }
  },

  players: {
    getById: async (id, signal) => {
      const response = await fetch(`${BASE_URL}/players/${id}`, { signal });
      if (!response.ok) throw new Error('Failed to fetch player');
      return response.json();
    }
  }
};

