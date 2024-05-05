class QueryGenerator:
    """ Generates queries for data extraction. """

    PERSON_TABLE = "person"
    GENRE_TABLE = "genre"
    FILMWORK_TABLE = "film_work"
    PERSON_FILM_WORK_TABLE = "person_film_work"
    GENRE_FILM_WORK_TABLE = "genre_film_work"
    MODIFIED_FIELD = "modified"

    def __init__(self, schema, modified):
        """ QueryGenerator class constructor. """

        self.schema = schema
        self.modified = modified

    def generate_persons_query(self):
        return f'SELECT id, full_name ' \
               f'FROM {self.schema}.{self.PERSON_TABLE} ' \
               f'WHERE {self.MODIFIED_FIELD} > \'{self.modified}\';'

    def generate_genre_query(self):
        return f'SELECT id, name ' \
               f'FROM {self.schema}.{self.GENRE_TABLE} ' \
               f'WHERE {self.MODIFIED_FIELD} > \'{self.modified}\';'

    def generate_person_filmwork_query(self, person_ids):
        return f'SELECT fw.id, fw.modified ' \
               f'FROM {self.schema}.{self.FILMWORK_TABLE} fw ' \
               f'LEFT JOIN {self.schema}.{self.PERSON_FILM_WORK_TABLE} pfw ON pfw.film_work_id = fw.id ' \
               f'WHERE pfw.person_id IN {person_ids};'

    def generate_genre_filmwork_query(self, genre_ids):
        return f'SELECT fw.id, fw.updated_at ' \
               f'FROM {self.schema}.{self.FILMWORK_TABLE} fw ' \
               f'LEFT JOIN {self.schema}.{self.GENRE_FILM_WORK_TABLE} gfw ON gfw.film_work_id = fw.id ' \
               f'WHERE gfw.genre_id IN {genre_ids};'

    def generate_filmwork_query(self, filmwork_ids):
        query = f'SELECT ' \
                f'fw.id as fw_id, ' \
                f'fw.title, ' \
                f'fw.description, ' \
                f'fw.rating, ' \
                f'fw.type, ' \
                f'fw.created, ' \
                f'fw.modified, ' \
                f'COALESCE ( ' \
                f'    json_agg( ' \
                f'        DISTINCT jsonb_build_object( ' \
                f'            \'person_role\', pfw.role, ' \
                f'            \'person_id\', p.id, ' \
                f'            \'person_name\', p.full_name ' \
                f'         ) ' \
                f'    ) FILTER (WHERE p.id is not null), ' \
                f'    \'[]\' ' \
                f') as persons, ' \
                f'array_agg(DISTINCT g.name) as genres ' \
            f'FROM content.film_work fw ' \
            f'LEFT JOIN {self.schema}.{self.PERSON_FILM_WORK_TABLE} pfw ON pfw.film_work_id = fw.id ' \
            f'LEFT JOIN {self.schema}.{self.PERSON_TABLE} p ON p.id = pfw.person_id ' \
            f'LEFT JOIN {self.schema}.{self.GENRE_FILM_WORK_TABLE} gfw ON gfw.film_work_id = fw.id ' \
            f'LEFT JOIN {self.schema}.{self.GENRE_TABLE} g ON g.id = gfw.genre_id ' \

        if filmwork_ids:
            query += f'WHERE fw.id IN {filmwork_ids} OR fw.{self.MODIFIED_FIELD} > \'{self.modified}\'' \
                     f'GROUP BY fw.id;'
        else:
            query += f'WHERE fw.{self.MODIFIED_FIELD} > \'{self.modified}\'' \
                     f'GROUP BY fw.id;'

        return query
