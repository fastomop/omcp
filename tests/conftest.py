"""
Pytest configuration file.
Contains shared fixtures and configuration for the test suite.
"""

import sys
import os

# Add the src directory to the path so we can import modules correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
