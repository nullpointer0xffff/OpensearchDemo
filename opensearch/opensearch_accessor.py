from opensearchpy import OpenSearch, exceptions
from opensearch.opensearch_types import RepoConfig, Event, Operation, Status
from utils.opensearch_utils import OpensearchUtils



class OpenSearchRepository:
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
        try:
            self.client.indices.create(index=index_name, ignore=400)
        except Exception as e:
            print("Failed to create index due to {e}")
            raise e

    def insert(self, document: Event, index_name: str):
        """Insert document to index"""
        if not self._does_index_exists(index_name):
            print(f"Index {index_name} doesn't exist, create one!")        
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

    def search(self, 
               query: dict) -> dict:
        # TODO: add try - catch logic here
        response = self.client.search(index=query, body=query)
        return response
