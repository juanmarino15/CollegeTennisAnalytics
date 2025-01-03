from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import matches, teams, players, schools
from .database import engine, Base

Base.metadata.create_all(bind=engine)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"])
app.include_router(matches.router)
app.include_router(teams.router)
app.include_router(players.router)
app.include_router(schools.router)