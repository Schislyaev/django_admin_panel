"""
Yandex.Practicum sprint 3.

Pydantic tables section

Author: Petr Schislyaev
Date: 11/11/2023
"""


from pydantic import BaseModel, validator
from typing import Optional, Dict


class ElasticIndex(BaseModel):
    _id: str
    id: str
    imdb_rating: float | None
    genre: Optional[list]
    title: str
    description: str | None
    director: str | None
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    actors: Optional[list[Dict]]
    writers: Optional[list[Dict]]

