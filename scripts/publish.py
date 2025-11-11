#!/usr/bin/env python3
"""
Script to prepare and publish the package to PyPI.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a shell command and handle errors."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error: {description} failed")
        print(result.stderr)
        return False
    
    print(result.stdout)
    print(f"✅ {description} completed successfully")
    return True


def main():
    """Main publishing workflow."""
    print("Dependency Metrics - PyPI Publishing Script")
    print("="*60)
    
    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Error: pyproject.toml not found. Run this script from the project root.")
        sys.exit(1)
    
    # Step 1: Clean previous builds
    print("\n🧹 Cleaning previous builds...")
    for path in ["build", "dist", "*.egg-info"]:
        if Path(path).exists():
            import shutil
            shutil.rmtree(path, ignore_errors=True)
    
    # Step 2: Install build tools
    if not run_command(
        [sys.executable, "-m", "pip", "install", "--upgrade", "build", "twine"],
        "Installing build tools"
    ):
        sys.exit(1)
    
    # Step 3: Run tests (if pytest is available)
    try:
        import pytest
        if not run_command(
            [sys.executable, "-m", "pytest", "tests/", "-v"],
            "Running tests"
        ):
            print("\n⚠️  Tests failed. Continue anyway? (y/n): ", end="")
            if input().lower() != 'y':
                sys.exit(1)
    except ImportError:
        print("\n⚠️  pytest not found. Skipping tests.")
    
    # Step 4: Build distribution
    if not run_command(
        [sys.executable, "-m", "build"],
        "Building distribution packages"
    ):
        sys.exit(1)
    
    # Step 5: Check distribution
    if not run_command(
        [sys.executable, "-m", "twine", "check", "dist/*"],
        "Checking distribution packages"
    ):
        sys.exit(1)
    
    # Step 6: Ask what to do next
    print("\n" + "="*60)
    print("Build completed successfully!")
    print("="*60)
    print("\nNext steps:")
    print("1. Test PyPI: Upload to test.pypi.org first")
    print("2. Production PyPI: Upload to pypi.org")
    print("3. Cancel: Exit without uploading")
    print()
    print("Enter your choice (1/2/3): ", end="")
    
    choice = input().strip()
    
    if choice == "1":
        # Upload to Test PyPI
        print("\n📦 Uploading to Test PyPI...")
        print("You'll need to enter your Test PyPI credentials.")
        if not run_command(
            [sys.executable, "-m", "twine", "upload", "--repository", "testpypi", "dist/*"],
            "Uploading to Test PyPI"
        ):
            sys.exit(1)
        
        print("\n" + "="*60)
        print("✅ Upload to Test PyPI successful!")
        print("="*60)
        print("\nTest the package with:")
        print("pip install --index-url https://test.pypi.org/simple/ dependency-metrics")
        
    elif choice == "2":
        # Upload to Production PyPI
        print("\n⚠️  WARNING: You're about to upload to production PyPI!")
        print("This action cannot be undone for this version.")
        print("\nContinue? (yes/no): ", end="")
        
        if input().strip().lower() == "yes":
            print("\n📦 Uploading to PyPI...")
            print("You'll need to enter your PyPI credentials.")
            if not run_command(
                [sys.executable, "-m", "twine", "upload", "dist/*"],
                "Uploading to PyPI"
            ):
                sys.exit(1)
            
            print("\n" + "="*60)
            print("✅ Upload to PyPI successful!")
            print("="*60)
            print("\nInstall the package with:")
            print("pip install dependency-metrics")
        else:
            print("❌ Upload cancelled.")
    
    else:
        print("❌ No upload performed.")
    
    print("\n" + "="*60)
    print("Publishing script completed!")
    print("="*60)


if __name__ == "__main__":
    main()
