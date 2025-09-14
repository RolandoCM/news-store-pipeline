from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class NewsArticle(BaseModel):
    title:str
    content:str
    sumary:Optional[str] = None
    source: str
    author: Optional[str] = None
    url: str
    published_at: datetime
    category: Optional[str] = None
    language: str
    tags:List = Field(default_factory=list)