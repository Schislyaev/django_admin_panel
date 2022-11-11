"""
Yandex.Practicum sprint 3.

Pydantic tables section

Author: Petr Schislyaev
Date: 11/11/2023
"""


from pydantic import BaseModel, validator
from typing import Optional


class NestedRoles(BaseModel):
    id: Optional[str]
    name: Optional[str]


class Actors(NestedRoles):
    pass


class Writers(NestedRoles):
    pass


class ElasticIndex(BaseModel):
    id: str
    imdb_rating: float | None
    genre: Optional[list[str]]
    title: str
    description: str | None
    director: str | None
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    actors: Optional[Actors]
    writers: Optional[Writers]

