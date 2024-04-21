import pdb
from typing import Callable, Any, Optional, List

from psycopg2.extras import RealDictRow
from pydantic import BaseModel


class Person(BaseModel):
    id: str
    name: str


class Filmwork(BaseModel):
    id: str
    title: str
    description: Optional[str]
    imdb_rating: Optional[float]
    genres: List[str | None]
    directors_names: List[str] = []
    directors: Optional[List[Person]]
    writers_names: List[str]
    writers: Optional[List[Person]]
    actors_names: List[str]
    actors: Optional[List[Person]]


class Transformer:
    """ Transform data from PostgreSQL DB."""

    def __init__(self, result_handler: Callable) -> None:
        """ Transformer class constructor. """

        self.result_handler = result_handler

    def transform_filmworks(self, filmworks_query_result: list) -> None:
        """ Transform movies data. """

        transformed_filmworks = []
        for filmwork in filmworks_query_result:
            directors, directors_names = self.collect_directors_data(filmwork)
            actors, actors_names = self.collect_actors_data(filmwork)
            writers, writer_names = self.collect_writers_data(filmwork)

            transformed_filmwork = Filmwork(
                id=filmwork["fw_id"],
                title=filmwork["title"],
                description=filmwork["description"],
                imdb_rating=filmwork["rating"],
                genres=filmwork["genres"],
                director_names=directors_names,
                directors=directors,
                writers_names=writer_names,
                writers=writers,
                actors_names=actors_names,
                actors=actors,
            )

            transformed_filmworks.append(transformed_filmwork)

        self.result_handler(transformed_filmworks)

    @staticmethod
    def collect_directors_data(filmwork: RealDictRow) -> tuple[list[Person], list[Any]]:
        """ Collect data about directors. """

        directors = []
        directors_names = []
        for person in filmwork["persons"]:
            if person["person_role"] == "director":
                directors_names.append(person["person_name"])
                directors.append(Person(
                    id=person["person_id"],
                    name=person["person_name"]
                ))

        return directors, directors_names

    @staticmethod
    def collect_actors_data(filmwork: RealDictRow) -> tuple[list[Person], list[Any]]:
        """ Collect data about actors. """

        actors = []
        actors_names = []
        for person in filmwork["persons"]:
            if person["person_role"] == "actor":
                actors_names.append(person["person_name"])
                actors.append(Person(
                    id=person["person_id"],
                    name=person["person_name"]
                ))

        return actors, actors_names

    @staticmethod
    def collect_writers_data(filmwork: RealDictRow) -> tuple[list[Person], list[Any]]:
        """ Collect data about writers. """

        writers = []
        writers_names = []
        for person in filmwork["persons"]:
            if person["person_role"] == "writer":
                writers_names.append(person["person_name"])
                writers.append(Person(
                    id=person["person_id"],
                    name=person["person_name"]
                ))

        return writers, writers_names
