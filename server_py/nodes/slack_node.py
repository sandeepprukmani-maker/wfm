import requests
from .registry import BaseNode, register_node
from ..utils import log, resolve_variables
from datetime import datetime

@register_node('slack_notification')
class SlackNotificationNode(BaseNode):
    def execute(self):
        webhook_url = resolve_variables(self.config.get('webhookUrl', ''), self.context)
        message = resolve_variables(self.config.get('message', 'Workflow notification'), self.context)
        
        credential_id = self.config.get('credentialId')
        if credential_id:
            cred = self.storage.get_credential(int(credential_id))
            if cred and cred.get('type') == 'slack':
                cred_data = cred.get('data', {})
                webhook_url = cred_data.get('webhookUrl', webhook_url)

        if not webhook_url:
            raise Exception("Slack Webhook URL is missing")

        self.logs.append({
            'timestamp': datetime.now().isoformat(), 
            'level': 'INFO', 
            'message': f"Sending Slack notification to {webhook_url[:20]}..."
        })
        
        response = requests.post(webhook_url, json={"text": message})
        response.raise_for_status()
        
        return {'status': 'success', 'response': response.text}
