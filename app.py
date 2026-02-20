"""
NorthLadder Asset Mapping Tool - Streamlit Cloud Entry Point

This is a thin wrapper for Streamlit Cloud deployment.
The actual app code is in src/app.py for better project organization.

Streamlit Cloud expects app.py in the root directory, so this file
executes the main app from src/.
"""

import sys
from pathlib import Path
import runpy

# Add src directory to Python path so imports work correctly
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Execute the main app from src/app.py
# Using runpy to avoid import issues and run it as a script
runpy.run_path(str(src_path / "app.py"), run_name="__main__")
