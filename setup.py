#!/usr/bin/env python3
"""
Setup script for the Artlist/MotionArray Bulk Downloader.
"""

import subprocess
import sys
import os

def install_requirements():
    """Install required packages."""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ All dependencies installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False

def check_environment():
    """Check if the environment is properly set up."""
    print("Checking environment...")
    
    try:
        import requests
        print("✓ requests library available")
    except ImportError:
        print("✗ requests library not available")
        return False
    
    try:
        import snowflake.connector
        print("✓ snowflake-connector-python available")
    except ImportError:
        print("⚠ snowflake-connector-python not available (optional for Snowflake integration)")
    
    try:
        from google.cloud import secretmanager
        print("✓ google-cloud-secret-manager available")
    except ImportError:
        print("⚠ google-cloud-secret-manager not available (optional for GCP authentication)")
    
    return True

def main():
    """Main setup function."""
    print("=== BULK DOWNLOADER SETUP ===")
    print("Setting up the Artlist/MotionArray bulk downloader...")
    print()
    
    # Install requirements
    if not install_requirements():
        print("Setup failed. Please install dependencies manually:")
        print("pip install -r requirements.txt")
        return False
    
    print()
    
    # Check environment
    if not check_environment():
        print("Environment check failed.")
        return False
    
    print()
    print("=== SETUP COMPLETE ===")
    print("You can now run the bulk downloader:")
    print("  python bulk_downloader.py              # Download from both platforms")
    print("  python bulk_downloader.py --test       # Test with example keys")
    print("  python test_downloader.py              # Run test suite")
    print()
    print("For Snowflake integration, ensure you have:")
    print("  1. Google Cloud credentials configured")
    print("  2. Access to the Snowflake database")
    print("  3. Proper API authentication for Artlist/MotionArray")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
