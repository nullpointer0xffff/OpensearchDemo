from opensearchpy import OpenSearch
import json
from datetime import datetime, timedelta
import random

username = "admin"
password = "Justatest@2024"
host = 'localhost'
port = 9200
auth = (username, password) # For testing only. Don't store credentials in code.

def generate_random_timestamp():
    end = datetime.now()
    start = end - timedelta(days=30)
    return start + (end - start) * random.random()

# Function to generate a random log entry
def generate_log_entry(index, _id):
    operations = ['INSERT', 'UPDATE', 'DELETE', 'SELECT']
    statuses = ['SUCCESS', 'FAILURE', 'PENDING']
    descriptions = [
        'Operation completed successfully.',
        'Operation failed due to timeout.',
        'Operation is pending approval.',
        'Operation completed with warnings.',
        'Operation failed due to syntax error.'
    ]
    
    log_entry = {
        'timestamp': generate_random_timestamp().isoformat(),
        'operation': random.choice(operations),
        'status': random.choice(statuses),
        'description': random.choice(descriptions)
    }
    
    return json.dumps({ "index" : { "_index" : index, "_id" : _id } }) + '\n' + json.dumps(log_entry) + '\n'


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
    
        # Generate 100 log entries
    index_name = 'mocklogs'
    bulk_data = ''
    for i in range(1, 101):
        bulk_data += generate_log_entry(index_name, i)  
    
    response = client.bulk(body=bulk_data)
    print(json.dumps(response, indent=2))
    print("Succeed!")
    
if __name__ == "__main__":
    run()