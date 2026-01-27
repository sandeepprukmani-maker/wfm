#!/usr/bin/env python3
import subprocess
import sys
import os
import signal
import time

processes = []

def cleanup(signum, frame):
    print("\nShutting down...")
    for p in processes:
        try:
            p.terminate()
        except:
            pass
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

def main():
    os.environ['PORT'] = '5001'
    
    flask_cmd = [sys.executable, '-m', 'server_py.main']
    flask_process = subprocess.Popen(flask_cmd, cwd=os.getcwd())
    processes.append(flask_process)
    print("Started Flask API on port 5001")
    
    time.sleep(1)
    
    vite_cmd = ['npx', 'vite', '--host', '0.0.0.0', '--port', '5000']
    vite_process = subprocess.Popen(vite_cmd, cwd=os.getcwd())
    processes.append(vite_process)
    print("Started Vite dev server on port 5000")
    
    try:
        while True:
            if flask_process.poll() is not None:
                print("Flask process exited")
                break
            if vite_process.poll() is not None:
                print("Vite process exited")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cleanup(None, None)

if __name__ == '__main__':
    main()
