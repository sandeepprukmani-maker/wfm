import os
import re
import json
from datetime import datetime, timedelta
from openai import OpenAI

openai_client = None

def get_ai():
    global openai_client
    if not openai_client:
        try:
            # First try the user's custom token fetching logic
            from server_py.fetch_token import fetch_token
            token_data = fetch_token()
            api_key = token_data.get('access_token')
            base_url = token_data.get('baseUrl')
            
            # Only use if tokens look valid (not placeholders)
            if api_key and base_url and "your_access_token_here" not in api_key and "your_base_url_here" not in base_url:
                openai_client = OpenAI(
                    api_key=api_key,
                    base_url=base_url
                )
                log("Using custom fetch_token for AI.")
            else:
                raise ValueError("Valid tokens not found in fetch_token response")
        except (ImportError, ValueError, Exception) as e:
            # Fallback to Replit AI Integration
            api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY')
            base_url = os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
            
            if api_key and base_url:
                openai_client = OpenAI(
                    api_key=api_key,
                    base_url=base_url
                )
                log("Using Replit AI Integration for OpenAI.")
            else:
                # Last resort: try standard OpenAI env var
                openai_client = OpenAI()
                log("Using default OpenAI client configuration.")
    return openai_client

def log(message, source='flask'):
    formatted_time = datetime.now().strftime('%I:%M:%S %p')
    print(f"{formatted_time} [{source}] {message}")

def resolve_variables(text, context):
    if not text or not isinstance(text, str):
        return text
    
    resolved = text
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    resolved = resolved.replace('{{today}}', today.strftime('%Y-%m-%d'))
    resolved = resolved.replace('{{yesterday}}', yesterday.strftime('%Y-%m-%d'))
    
    date_pattern = r'\{\{date:([^:]+):?([^}]*)\}\}'
    def replace_date(match):
        fmt = match.group(1)
        modifier = match.group(2) or ''
        date = datetime.now()
        if modifier.startswith('sub'):
            days = int(modifier.replace('sub', ''))
            date = date - timedelta(days=days)
        return date.strftime(fmt.replace('yyyy', '%Y').replace('MM', '%m').replace('dd', '%d'))
    
    resolved = re.sub(date_pattern, replace_date, resolved)
    
    for key, value in context.items():
        pattern = re.compile(r'\{\{' + re.escape(key) + r'\}\}', re.IGNORECASE)
        if isinstance(value, (str, int, float)):
            resolved = pattern.sub(str(value), resolved)
        elif isinstance(value, dict):
            resolved = pattern.sub(json.dumps(value), resolved)
    
    return resolved
