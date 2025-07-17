#!/usr/bin/env python3
"""
Test script to verify the refactored pipeline structure.
Runs a minimal set of scripts to ensure basic functionality.
"""

import subprocess
import sys
from pathlib import Path
from colorama import init, Fore, Style

# Initialize colorama
init()

SCRIPTS_DIR = Path(__file__).parent
PYTHON_PATH = "~/miniconda3/envs/grocery_poc/bin/python"

def run_script(script_name):
    """Run a single script and return success status"""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"  {Fore.RED}✗{Style.RESET_ALL} Script not found: {script_name}")
        return False
    
    print(f"\n{Fore.CYAN}Running {script_name}...{Style.RESET_ALL}")
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Success")
            return True
        else:
            print(f"  {Fore.RED}✗{Style.RESET_ALL} Failed with return code {result.returncode}")
            if result.stderr:
                print(f"  Error: {result.stderr[:200]}...")
            return False
    except subprocess.TimeoutExpired:
        print(f"  {Fore.YELLOW}⚠{Style.RESET_ALL} Timeout after 60 seconds")
        return False
    except Exception as e:
        print(f"  {Fore.RED}✗{Style.RESET_ALL} Exception: {e}")
        return False

def main():
    """Test the basic pipeline structure"""
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Testing Refactored Pipeline Structure{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    
    # Test scripts in order
    test_scripts = [
        ("00_environment_setup.py", "Environment validation"),
        ("01_validate_databases.py", "Database validation"),
        ("10_load_m5_dataset.py", "M5 dataset loading"),
        ("12_load_products.py", "Product loading"),
    ]
    
    results = []
    
    for script, description in test_scripts:
        print(f"\nTest: {description}")
        success = run_script(script)
        results.append((script, success))
        
        if not success:
            print(f"\n{Fore.YELLOW}Stopping tests due to failure{Style.RESET_ALL}")
            break
    
    # Summary
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Test Summary{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for script, success in results:
        status = f"{Fore.GREEN}PASS{Style.RESET_ALL}" if success else f"{Fore.RED}FAIL{Style.RESET_ALL}"
        print(f"{script:<30} [{status}]")
    
    print(f"\nPassed: {passed}/{total}")
    
    if passed == total:
        print(f"\n{Fore.GREEN}✓ All tests passed!{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.RED}✗ Some tests failed{Style.RESET_ALL}")
        print("\nNote: This is a basic structure test. Full pipeline testing")
        print("requires running all scripts in sequence with proper data.")

if __name__ == "__main__":
    main()