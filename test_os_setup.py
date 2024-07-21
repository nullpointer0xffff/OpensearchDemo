from opensearchpy import OpenSearch


username = "admin"
password = "Justatest@2024"
host = 'localhost'
port = 9200
auth = (username, password) # For testing only. Don't store credentials in code.



def run():
    # Create the client with SSL/TLS enabled, but hostname verification disabled.
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    
    # Delete Index
    # response = client.indices.delete(
    #     index = 'python-test-index'
    # )
    
    # Create index
    index_name = 'python-test-index'
    index_body = {
        'settings': {
            'index': {
            'number_of_shards': 4
            }
        }
    }
    
    response = client.indices.create(index_name, body=index_body)
    print(response)
    
    
    # Indexing doc
    document = {
        'title': 'Moneyball',
        'director': 'Bennett Miller',
        'year': '2011'
    }

    response = client.index(
        index = 'python-test-index',
        body = document,
        id = '1',
        refresh = True
    )
    
    q = 'miller'
    query = {
        'size': 5,
        'query': {
                'multi_match': {
                'query': q,
                'fields': ['title^2', 'director']
                }
            }
    }

    response = client.search(
        body = query,
        index = 'python-test-index'
    )
    print("Search Response:")
    print(response)    
    

    
if __name__ == "__main__":
    run()
