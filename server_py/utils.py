import os
import re
import json
from datetime import datetime, timedelta
from openai import OpenAI

openai_client = None

def get_ai():
    global openai_client
    if not openai_client:
        openai_client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
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
