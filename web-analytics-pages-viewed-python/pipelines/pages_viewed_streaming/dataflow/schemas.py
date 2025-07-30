# dataflow/schemas.py
from typing import NamedTuple, Optional
import apache_beam as beam

class GA4Event(NamedTuple):
    user_pseudo_id: Optional[str]
    session_id: Optional[str] 
    user_id: Optional[str]
    subscriber_number: Optional[str]
    event_name: str
    event_timestamp: str
    page_url: Optional[str]
    customer_id: Optional[str]

beam.coders.registry.register_coder(GA4Event, beam.coders.RowCoder)
