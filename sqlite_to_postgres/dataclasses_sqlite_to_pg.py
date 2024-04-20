import datetime
import uuid
from dataclasses import dataclass, field


@dataclass
class Movie:
    title: str
    description: str
    creation_date: datetime.datetime
    type: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)


@dataclass
class Genre:
    name: str
    description: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreMovie:
    film_work_id: str
    genre_id: str
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonMovie:
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
