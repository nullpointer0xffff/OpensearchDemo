from datetime import datetime
from opensearch.opensearch_types import Operation, Status, Event


class OpensearchUtils:
    """Utility / helper functions for opensearch queries"""
    @classmethod
    def build_index(cls, warehouse_id, connection_id):
        return f"{warehouse_id}-{connection_id}"
    
    @classmethod
    def extract_search_result(cls, response: dict) -> list[Event]:
        return response['hits']['hits']
    
    @classmethod
    def _to_timestamp(cls, timestamp:str):
        """ Validate the timestamp format. Returns the timestamp if valid, otherwise raises ValueError. """
        try:
            # Assuming the expected timestamp format is "YYYY-MM-DD"
            return datetime.strptime(timestamp, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Timestamp format must be YYYY-MM-DD")

    @classmethod
    def build_query(cls, 
               index_name: str,
               operation: Operation | None = None, 
               status: Status | None = None, 
               start_time: str | None = None, 
               end_time: str | None = None) -> dict:

        warehouse_id, connection_id = index_name.split('-')

        query = {
            "bool": {
                "filter": []
            }
        }
        
        filters = [
            {
                "term": {"warehouse_id": warehouse_id,}
            },
            {
                "term": {"connection_id": connection_id,}
            },
        ]
        
        if start_time or end_time:
            _time_query = {}
            if start_time:
                _time_query["gte"] = cls._to_timestamp(start_time)
            if end_time:
                _time_query["lte"] = cls._to_timestamp(end_time)
            if _time_query:
                filters.append(_time_query)
                
        if status:
            filters.append({
                "term": {
                    "status": status
                }
            })
        
        # Add operation to the filter if provided
        if operation:
            filters.append({
                "term": {
                    "operation": operation
                }
            })

        # Build the query using the accumulated filters
        query = {
            "query": {
                "bool": {
                    "filter": filters
                }
            }
        }
        return query
