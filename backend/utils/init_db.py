"""
Database initialization script
Creates all tables and optionally seeds with sample data
"""
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from database import create_tables, drop_tables, SessionLocal
from models import *


def init_database():
    """Initialize the database by creating all tables"""
    print("Creating database tables...")
    create_tables()
    print("Database tables created successfully!")


def reset_database():
    """Reset the database by dropping and recreating all tables"""
    print("Dropping existing tables...")
    drop_tables()
    print("Creating new tables...")
    create_tables()
    print("Database reset successfully!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database management script")
    parser.add_argument("--init", action="store_true", help="Initialize database")
    parser.add_argument("--reset", action="store_true", help="Reset database")
    
    args = parser.parse_args()
    
    if args.reset:
        reset_database()
    elif args.init:
        init_database()