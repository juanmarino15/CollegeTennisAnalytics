from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import matches, teams, players, schools,stats,seasons
from .database import engine, Base

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="College Tennis API",
    description="API for college tennis matches, teams, players, and statistics",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers with versioning and tags
app.include_router(matches.router, prefix="/api/v1/matches", tags=["matches"])
app.include_router(teams.router, prefix="/api/v1/teams", tags=["teams"])
app.include_router(players.router, prefix="/api/v1/players", tags=["players"])
app.include_router(schools.router, prefix="/api/v1/schools", tags=["schools"])
app.include_router(stats.router, prefix="/api/v1/stats", tags=["stats"])
app.include_router(seasons.router, prefix="/api/v1/seasons", tags=["seasons"]) 


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)