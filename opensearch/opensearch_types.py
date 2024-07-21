from enum import Enum
from pydantic import BaseModel
from datetime import datetime


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
    timestamp: datetime
    operation: Operation
    status: Status
    description: str


class RepoConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
