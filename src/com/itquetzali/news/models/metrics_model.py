from datetime import datetime
from typing import Optional

class MetricsModel:
    total_articles: int = 0
    cassandra_count: int = 0
    iceberg_count: int = 0
    last_processed: Optional[datetime] = None
    processing_time_ms: float = 0.0