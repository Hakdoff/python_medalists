from pydantic import BaseModel
from typing import List, Optional

class Medalist(BaseModel):
    name: str
    medal_type: str
    gender: str
    country: str
    country_code: str
    nationality: str
    medal_code: str
    medal_date: str
    
class AggregatedStats(BaseModel):
    discipline: str
    event: str
    event_date: str
    medalists: List[Medalist]