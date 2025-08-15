# test_single_tournament.py
"""
Test script to collect draw data for a single tournament
Run this from the migrations directory: python test_single_tournament.py
"""

import os
import sys
import logging
from pathlib import Path

# Simple and robust path resolution when running from migrations folder
current_file = Path(__file__).resolve()
migrations_dir = current_file.parent  # migrations/
scripts_dir = migrations_dir.parent   # scripts/
backend_dir = scripts_dir.parent      # backend/

print(f"Script: {current_file}")
print(f"Backend: {backend_dir}")

# Add backend directory to Python path
sys.path.insert(0, str(backend_dir))

# Verify models exist and import
models_file = backend_dir / 'models' / 'models.py'
if not models_file.exists():
    print(f"❌ ERROR: models.py not found at {models_file}")
    sys.exit(1)

try:
    from models.models import Base, Tournament, TournamentEvent
    print("✅ Successfully imported models")
except ImportError as e:
    print(f"❌ Failed to import models: {e}")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_single_tournament.log'),
        logging.StreamHandler()
    ]
)

def test_single_tournament():
    """Test tournament draw collection for a single tournament"""
    
    # Database URL
    database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    
    # Test tournament ID
    test_tournament_id = "92BC5EA2-B793-4E41-8252-9838A350538E"
    
    try:
        from collector.tournament_draw_visualization_collector import TournamentDrawVisualizationCollector
        
        logging.info(f"Testing tournament draw collection for: {test_tournament_id}")
        
        # Initialize collector
        collector = TournamentDrawVisualizationCollector(database_url)
        
        # Test the collection for this specific tournament
        collector.collect_draws_for_tournament_events(test_tournament_id)
        
        logging.info("✅ Test completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_single_tournament()