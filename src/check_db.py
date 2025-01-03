from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from models.models import Base, Match, Team, MatchTeam, WebLink  # Changed this line

# Setup the PostgreSQL database connection
DATABASE_URL = "postgresql://juanmarino:promocion2011@localhost:5432/college_tennis_db"  # Replace with your PostgreSQL credentials and database details
engine = create_engine(DATABASE_URL)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Query the teams table and load the results into a pandas DataFrame
    query = session.query(Team)  # You can query the Team model directly here
    teams_df = pd.read_sql(query.statement, session.bind)  # Use the query statement
    
    # Display the dataframe
    print(teams_df)

finally:
    # Close the session after the query
    session.close()
