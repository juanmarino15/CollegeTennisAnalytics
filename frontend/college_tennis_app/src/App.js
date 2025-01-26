import React, { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Home, Users, Calendar, Award, Menu, Sun, Moon } from 'lucide-react';
import HomePage from './pages/HomePage';
import MatchesPage from './pages/MatchesPage';
import MatchDetailsPage from './pages/MatchDetailsPage';
// import TeamsPage from './pages/TeamsPage';
// import RankingsPage from './pages/RankingsPage';

// Theme toggle component
const ThemeToggle = () => {
  const [isDark, setIsDark] = useState(true);

  useEffect(() => {
    document.documentElement.classList.add('dark');
  }, []);

  const toggleTheme = () => {
    setIsDark(!isDark);
    document.documentElement.classList.toggle('dark');
  };

  return (
    <button 
      onClick={toggleTheme}
      className="p-2 rounded-full text-gray-600 dark:text-dark-text hover:bg-gray-100 dark:hover:bg-dark-card"
    >
      {isDark ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
    </button>
  );
};

// NavLink component for navigation items
const NavLink = ({ to, icon, label }) => {
  const location = useLocation();
  const isActive = location.pathname === to;

  return (
    <Link 
      to={to} 
      className={`flex flex-col items-center py-2 px-3 
        ${isActive 
          ? 'text-primary-500 dark:text-primary-400' 
          : 'text-gray-600 dark:text-dark-text-dim'
        }
        hover:text-primary-600 dark:hover:text-primary-300`}
    >
      {React.cloneElement(icon, { className: "w-6 h-6" })}
      <span className="text-xs mt-1">{label}</span>
    </Link>
  );
};

// Layout Component
const Layout = ({ children }) => {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-dark-bg transition-colors duration-200">
      {/* Mobile Header */}
      <header className="sticky top-0 z-10 bg-white dark:bg-dark-nav border-b border-gray-200 dark:border-dark-border">
        <div className="px-4 h-14 flex items-center justify-between">
          <h1 className="text-lg font-semibold text-gray-900 dark:text-dark-text">College Tennis</h1>
          <div className="flex items-center gap-2">
            <ThemeToggle />
            <button className="p-2 hover:bg-gray-100 dark:hover:bg-dark-border rounded-full">
              <Menu className="w-5 h-5 text-gray-600 dark:text-dark-text" />
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="px-4 pb-24">
        {children}
      </main>

      {/* Mobile Navigation */}
      <nav className="fixed bottom-0 left-0 right-0 bg-white dark:bg-dark-nav border-t border-gray-200 dark:border-dark-border z-10">
        <div className="flex justify-around">
          <NavLink to="/" icon={<Home />} label="Home" />
          <NavLink to="/matches" icon={<Calendar />} label="Matches" />
          <NavLink to="/teams" icon={<Users />} label="Teams" />
          <NavLink to="/rankings" icon={<Award />} label="Rankings" />
        </div>
      </nav>
    </div>
  );
};

// Main App Component
const App = () => {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/matches" element={<MatchesPage />} />
          <Route path="/matches/:matchId" element={<MatchDetailsPage />} />
          {/* <Route path="/teams" element={<TeamsPage />} />
          <Route path="/rankings" element={<RankingsPage />} /> */}
        </Routes>
      </Layout>
    </BrowserRouter>
  );
};

export default App;