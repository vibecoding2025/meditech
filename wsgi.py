"""WSGI entry point for PythonAnywhere deployment."""

import sys
import os

# Add project directory to path
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

from app import app as application
import database as db

# Ensure database is initialized
db.init_db()
