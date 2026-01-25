import sys
import requests
import json

def fetch_lim_token(gateway_url, credentials):
    """
    Fetches an auth token from the LIM gateway.
    """
    try:
        response = requests.post(f"{gateway_url}/auth/token", json=credentials)
        response.raise_for_status()
        return response.json().get("token")
    except Exception as e:
        print(f"Error fetching token: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    URL = "https://lim-gateway.example.com"
    CREDS = {"user": "admin", "pass": "secret"}
    token = fetch_lim_token(URL, CREDS)
    if token:
        print(token)
