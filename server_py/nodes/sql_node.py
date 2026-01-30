import sqlalchemy
from sqlalchemy import text
from .registry import BaseNode, register_node
from ..utils import log, resolve_variables
from datetime import datetime

@register_node('sql_query')
class SQLQueryNode(BaseNode):
    def execute(self):
        query = resolve_variables(self.config.get('query', ''), self.context)
        credential_id = self.config.get('credentialId')
        self.logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Running SQL: {query}"})
        
        query_results = []
        try:
            if credential_id:
                cred = self.storage.get_credential(int(credential_id))
                if cred:
                    cred_type = cred.get('type')
                    cred_data = cred.get('data', {})
                    
                    if cred_type == 'mssql':
                        conn_str = f"mssql+pymssql://{cred_data.get('username')}:{cred_data.get('password')}@{cred_data.get('host')}:{cred_data.get('port', 1433)}/{cred_data.get('database')}"
                        engine = sqlalchemy.create_engine(conn_str)
                        with engine.connect() as conn:
                            result = conn.execute(text(query))
                            query_results = [dict(row._mapping) for row in result]
            else:
                from ..models import engine as internal_engine
                with internal_engine.connect() as conn:
                    result = conn.execute(text(query))
                    query_results = [dict(row._mapping) for row in result]
        except Exception as e:
            raise Exception(f"SQL Error: {str(e)}")

        record_count = len(query_results)
        return {'status': 'success', 'count': record_count, 'results': query_results}
