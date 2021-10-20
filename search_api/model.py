from typing import Optional
from pydantic import BaseModel


class UserNewsAttention(BaseModel):
    keywords: list
    news_id: str
    user_token: str
    attention_time: int
    timestamp: str
    before: str
    next: str


class NewsKeywords(BaseModel):
    keywords: list
    news_id: str
    timestamp: str


class Query(BaseModel):
    query: str
    page: int
    size: int
