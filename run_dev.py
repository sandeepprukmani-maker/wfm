#!/usr/bin/env python3
import subprocess
import sys
import os
import signal
import time
import socket

processes = []

def kill_process_on_port(port):
    """Kill any process listening on the specified port."""
    try:
        # Try to find processes using netstat/lsof if possible, but fallback to ps grep
        # This is for Replit environment specifically
        cmd = f"fuser -k {port}/tcp 2>/dev/null || lsof -ti:{port} | xargs kill -9 2>/dev/null || true"
        subprocess.run(cmd, shell=True)
        
        # Fallback ps grep
        cmd_ps = f"ps -ef | grep -E 'python|node|vite' | grep -v grep"
        output = subprocess.check_output(cmd_ps, shell=True).decode()
        for line in output.splitlines():
            if f":{port}" in line or f"port {port}" in line or f"--port {port}" in line:
                pid = int(line.split()[1])
                if pid != os.getpid():
                    print(f"Killing process {pid} associated with port {port}")
                    os.kill(pid, signal.SIGKILL)
    except Exception as e:
        pass

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
    # Pre-start cleanup to ensure ports are free
    print("Performing pre-start cleanup...")
    kill_process_on_port(5000)
    kill_process_on_port(5001)
    
    time.sleep(1)

    os.environ['PORT'] = '5001'
    
    flask_cmd = [sys.executable, '-m', 'server_py.main']
    flask_process = subprocess.Popen(flask_cmd, cwd=os.getcwd(), shell=os.name == 'nt')
    processes.append(flask_process)
    print("Started Flask API on port 5001")
    
    time.sleep(1)
    
    npx_cmd = 'npx.cmd' if os.name == 'nt' else 'npx'
    # Added --strictPort to ensure it fails if port 5000 is not available
    vite_cmd = [npx_cmd, 'vite', '--host', '0.0.0.0', '--port', '5000', '--strictPort']
    vite_process = subprocess.Popen(vite_cmd, cwd=os.getcwd(), shell=os.name == 'nt')
    processes.append(vite_process)
    print("Started Vite dev server on port 5000 (strict)")
    
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
