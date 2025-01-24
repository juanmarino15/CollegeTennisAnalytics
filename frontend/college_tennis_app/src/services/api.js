const BASE_URL = 'http://localhost:8000/api/v1';

export const api = {
  // Matches endpoints
  matches: {
    getAll: async (date) => {
      const params = date ? `?date=${date}` : '';
      const response = await fetch(`${BASE_URL}/matches${params}`);
      if (!response.ok) throw new Error('Failed to fetch matches');
      return response.json();
    },
    getById: async (id) => {
      const response = await fetch(`${BASE_URL}/matches/${id}`);
      if (!response.ok) throw new Error('Failed to fetch match');
      return response.json();
    },
    getLineup: async (id) => {
      const response = await fetch(`${BASE_URL}/matches/${id}/lineup`);
      if (!response.ok) throw new Error('Failed to fetch match lineup');
      return response.json();
    },
    getScore: async (id) => {
      const response = await fetch(`${BASE_URL}/matches/${id}/score`);
      if (!response.ok) throw new Error('Failed to fetch match score');
      return response.json();
    }
  },

  // Teams endpoints
  teams: {
    getAll: async () => {
      const response = await fetch(`${BASE_URL}/teams`);
      if (!response.ok) throw new Error('Failed to fetch teams');
      return response.json();
    },
    getById: async (id) => {
      const response = await fetch(`${BASE_URL}/teams/${id}`);
      if (!response.ok) throw new Error('Failed to fetch team');
      return response.json();
    },
    getLogo: async (id) => {
      const response = await fetch(`${BASE_URL}/teams/${id}/logo`);
      if (!response.ok) throw new Error('Failed to fetch team logo');
      return response.blob();
    }
  }
};