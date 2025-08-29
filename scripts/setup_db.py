#!/usr/bin/env python3
"""
Database setup and initialization script
"""
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

from database import init_db, engine
from models import Base

def main():
    print("Initializing database...")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    print("Database initialized successfully!")

if __name__ == "__main__":
    main()