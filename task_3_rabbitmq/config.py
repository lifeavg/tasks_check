from dataclasses import dataclass
from pathlib import Path


@dataclass
class _Config:
    wk_dir = Path('data')
    result_dir = Path('result')
    generator_delay = 5
    publisher_delay = 15
    queue = 'files'
    rabbit_connection = 'amqp://guest:guest@localhost:5672?heartbeat=60'


config = _Config
