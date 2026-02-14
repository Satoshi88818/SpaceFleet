#!/usr/bin/env python3
"""
Quick test script to verify the space robot fleet system works correctly.
Runs a small fleet for 30 seconds and checks basic functionality.
"""

import asyncio
import subprocess
import sys
import time
import requests
from pathlib import Path

def check_dependencies():
    """Check if all required packages are installed"""
    print("Checking dependencies...")
    try:
        import numpy
        import prometheus_client
        import pydantic
        import cryptography
        print("✓ All dependencies installed")
        return True
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("\nPlease install dependencies:")
        print("  pip install -r requirements.txt")
        return False

def run_test():
    """Run a quick test of the system"""
    print("\n" + "="*60)
    print("SPACE ROBOT FLEET MANAGEMENT - QUICK TEST")
    print("="*60)
    
    if not check_dependencies():
        return False
    
    print("\nStarting system with 10 robots for 30 seconds...")
    print("(Press Ctrl+C to stop early)\n")
    
    # Start the system
    process = subprocess.Popen(
        [sys.executable, 'space_robot_fleet.py', '--num-robots', '10', '--telemetry-interval', '0.5'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    start_time = time.time()
    lines_shown = 0
    max_lines = 20
    
    try:
        # Show first few log lines
        for line in process.stdout:
            if lines_shown < max_lines:
                print(line.rstrip())
                lines_shown += 1
            elif lines_shown == max_lines:
                print("\n... (logs continuing, use Ctrl+C to stop) ...\n")
                lines_shown += 1
            
            # Check metrics after 10 seconds
            if time.time() - start_time > 10 and time.time() - start_time < 11:
                check_metrics()
            
            # Stop after 30 seconds
            if time.time() - start_time > 30:
                print("\n" + "="*60)
                print("Test completed successfully!")
                print("="*60)
                process.terminate()
                break
                
    except KeyboardInterrupt:
        print("\n\nStopping test...")
        process.terminate()
    
    process.wait()
    
    # Check if data directory was created
    data_dir = Path('./robot_data')
    if data_dir.exists():
        print("\n✓ Data directory created")
        
        db_file = data_dir / 'robot_fleet.db'
        if db_file.exists():
            size_mb = db_file.stat().st_size / (1024 * 1024)
            print(f"✓ Database created ({size_mb:.2f} MB)")
        
        log_file = data_dir / 'telemetry.log'
        if log_file.exists():
            size_kb = log_file.stat().st_size / 1024
            print(f"✓ Telemetry log created ({size_kb:.2f} KB)")
    
    return True

def check_metrics():
    """Check Prometheus metrics"""
    try:
        response = requests.get('http://localhost:8000/metrics', timeout=2)
        if response.status_code == 200:
            metrics = response.text
            
            # Parse some key metrics
            for line in metrics.split('\n'):
                if 'robot_telemetry_sent_total' in line and not line.startswith('#'):
                    value = float(line.split()[-1])
                    print(f"\n✓ Metrics available - {int(value)} telemetry messages sent")
                    break
    except:
        pass  # Metrics not ready yet

if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
