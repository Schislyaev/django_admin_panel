"""
Yandex.Practicum sprint 3.

Pydantic tables section

Author: Petr Schislyaev
Date: 11/11/2023
"""


from typing import Dict, Optional

from pydantic import BaseModel, validator


class ElasticIndex(BaseModel):
    _id: str
    id: str
    imdb_rating: float | None
    genre: Optional[list]
    title: str
    description: Optional[str | None]
    director: list
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    actors: Optional[list[Dict]]
    writers: Optional[list[Dict]]
    modified: str

    @classmethod
    @validator('director')
    def validate_director(cls, value_to_validate):
        return value_to_validate if value_to_validate else []
