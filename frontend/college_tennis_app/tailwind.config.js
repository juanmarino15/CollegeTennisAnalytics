/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,jsx,ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        primary: {
          50: "#f0fdf4",
          100: "#dcfce7",
          200: "#bbf7d0",
          300: "#86efac",
          400: "#4ade80",
          500: "#22c55e",
          600: "#16a34a",
          700: "#15803d",
          800: "#166534",
          900: "#14532d",
        },
        dark: {
          bg: "#000000", // Pure black background
          nav: "#1d1d1d", // Lighter gray for nav elements (updated)
          card: "#111111", // Very dark gray for cards
          border: "#2d2d2d", // Slightly lighter borders (updated)
          text: "#ffffff",
          "text-dim": "#a0a0a0",
        },
      },
    },
  },
  plugins: [],
};
