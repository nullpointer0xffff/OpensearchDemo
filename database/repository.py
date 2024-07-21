from opensearchpy import OpenSearch, exceptions
from abc import ABC, abstractmethod
from enum import Enum
from pydantic import BaseModel
from datetime import date, datetime


class Operation(str, Enum):
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    SELECT = 'SELECT'

class Status(str, Enum):
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE' 
    PENDING = 'PENDING'


class Event(BaseModel):
    warehouse_id: str
    connection_id: str
    index_name: str
    timestamp: datetime
    operation: Operation
    status: Status
    description: text



class Repository(ABC):
    """Interface / abstraction of database operations"""
        
    @abstractmethod
    def create_index(self, index_name: str):
        """Create index"""
        
        
    @abstractmethod
    def insert(self, document: Event, index_name: str):
        """Insert document to index"""
    
    @abstractmethod
    def search(
        self, 
        index_name: str,
        operation: Operation | None = None, 
        status: Status | None = None,
        start_time: str | None = None, 
        end_time: str | None = None,
    ) -> list[Event]:
        """Search by conditions"""



class RepoConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str


class OpenSearchRepository(Repository):
    
    def __init__(self, repo_config: RepoConfig):
        # Should use dependency injection
        self.client = OpenSearch(
            hosts = [{'host': repo_config.host, 'port': repo_config.port}],
            http_compress = True, # enables gzip compression for request bodies
            http_auth = (repo_config.username, repo_config.password),
            use_ssl = True,
            verify_certs = False,
            ssl_assert_hostname = False,
            ssl_show_warn = False
        )

    def _does_index_exists(self, index_name: str):
        return self.client.indices.exists(index=index_name)
        
    def create_index(self, index_name: str):
        if self._does_index_exists(index_name):
            print(f"Index {index_name} exists, skip creation!")
            return

        self.client.indices.create(index=index_name, ignore=400)


    def insert(self, document: Event, index_name: str):
        """Insert document to index"""
        self.create_index(index_name)
        
        try:
            # TODO: add retries
            response = self.client.index(index=index_name, body=document.model_dump(), id=None)
            if 'result' in response and response['result'] != 'created':
                print("Failed to create document: " + str(response))
                return False
            print("Document indexed successfully:", response)
        except exceptions.ConflictError as e:
            print("Version conflict error:", e)
            return False
        except exceptions.ConnectionError as e:
            print("Connection error:", e)
            # TODO: should retry
            return False
        except Exception as e:
            breakpoint()
            print("Unknown error:", e)
            return False
        
        return True

    def _to_timestamp(self, timestamp:str):
        """ Validate the timestamp format. Returns the timestamp if valid, otherwise raises ValueError. """
        try:
            # Assuming the expected timestamp format is "YYYY-MM-DD"
            return datetime.strptime(timestamp, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Timestamp format must be YYYY-MM-DD")


    def _build_query(self, 
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
                _time_query["gte"] = self._to_timestamp(start_time)
            if end_time:
                _time_query["lte"] = self._to_timestamp(end_time)
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
        if self.client.indices.validate_query(query):
            return query
        raise ValueError(f"Illegal query: {query}")

    def search(self, 
               index_name: str,
               operation: Operation | None = None, 
               status: Status | None = None, 
               start_time: str | None = None, 
               end_time: str | None = None) -> list[Event]:
        
        _query = self._build_query(index_name, operation, status, start_time, end_time)
        # TODO: add try - catch logic here
        response = self.client.search(index=index_name, body=_query)
        return response['hits']['hits']
