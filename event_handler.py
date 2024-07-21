from database.repository import Repository, RepoConfig, OpenSearchRepository, Event
import random
from datetime import datetime, timedelta
from tqdm import tqdm

class EventHandler:
    def __init__(self, db_repository: Repository) -> None:
        self.db_repository = db_repository
        
    
    def process(self, events: list[Event]):
        if not events:
            return

        unique_indices = set([e.index_name for e in events])
        for id in unique_indices:
            self.db_repository.create_index(id)
            print(f"Created index {id}")

        for event in events:
            index_name = event.index_name
            self.db_repository.insert(event, index_name=index_name)
    
    
def generate_random_timestamp():
    end = datetime.now()
    start = end - timedelta(days=30)
    return start + (end - start) * random.random()


def generate_events(num_events: int) -> list[Event]:
    warehouse_ids = [f'warehouse_{i}' for i in range(1, 6)] 
    connection_ids = [f'connection_{i}' for i in range(1, 6)]
    operations = ['INSERT', 'UPDATE', 'DELETE', 'SELECT']
    statuses = ['SUCCESS', 'FAILURE', 'PENDING']
    descriptions = [
        'Operation completed successfully.',
        'Operation failed due to timeout.',
        'Operation is pending approval.',
        'Operation completed with warnings.',
        'Operation failed due to syntax error.'
    ]
    
    def _generate_one_event(wid, cid, time, operation, status, desc):
        
        log_entry = {
            'warehouse_id': wid,
            'connection_id': cid,
            'index_name': f"{wid}-{cid}",
            'timestamp': time,
            'operation': operation,
            'status': status,
            'description': desc,
        }
        
        return Event(**log_entry)
    
    ret = []
    for i in tqdm(range(num_events)):
        wid = random.choice(warehouse_ids)
        cid = random.choice(connection_ids)
        time = generate_random_timestamp().isoformat()
        operation = random.choice(operations)
        status = random.choice(statuses)
        desc = random.choice(descriptions)
        print(f"Generating event with {wid} and {cid}")
        ret.append(_generate_one_event(wid, cid, time, operation, status, desc))
    return ret


def test():
    repo_config = RepoConfig(
        username = "admin",
        password = "Justatest@2024",
        host = 'localhost',
        port = 9200,
    )
    
    db_repo = OpenSearchRepository(repo_config)
    event_handler = EventHandler(db_repo)
    
    
    test_events = generate_events(1000)
    event_handler.process(test_events)
    
    # Verify
    searched_events = db_repo.search(
        index_name='warehouse_2-connection_3',
        start_time="2024-01-03",
    )
    
    print(f"Success to get {len(searched_events)}")
    
if __name__ == "__main__":
    test()