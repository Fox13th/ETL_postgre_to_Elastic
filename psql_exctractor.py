import logging


def get_ids(pack_data):
    return tuple([id_pers[0] for id_pers in pack_data])


class PSQLExtractor:

    def __init__(self, connection):
        self.conn = connection
        self.curs = self.conn.cursor()
        self.log = logging.getLogger(self.__class__.__name__)

    def init_query(self, table, data):
        """
        Функция-запрос для отслеживания изменений в таблице
        :param table: имя таблицы
        :param data: значение нижней границы поля modified
        :return: результат запроса
        """
        match table:
            case 'film_work':
                query = f"SELECT fw.id, fw.title, fw.description, fw.rating, " \
                        f"ARRAY_AGG(DISTINCT genre.name::text) AS genres, " \
                        f"ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) " \
                        f"FILTER (WHERE person_film.role = 'director') AS directors, " \
                        f"ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) " \
                        f"FILTER (WHERE person_film.role = 'actor') AS actors, " \
                        f"ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) " \
                        f"FILTER (WHERE person_film.role = 'writer') AS writers, fw.modified " \
                        f"FROM content.film_work fw " \
                        f"LEFT JOIN content.genre_film_work AS genre_film ON fw.id = genre_film.film_work_id " \
                        f"LEFT JOIN content.genre AS genre ON genre_film.genre_id = genre.id " \
                        f"LEFT JOIN content.person_film_work AS person_film ON fw.id = person_film.film_work_id " \
                        f"LEFT JOIN content.person AS person ON person_film.person_id = person.id " \
                        f"WHERE fw.modified > '{data}' " \
                        f"GROUP BY fw.id ORDER BY fw.modified "
            case _:
                first_query = f"SELECT id, modified FROM content.{table} WHERE modified > '{data}' ORDER BY modified "
                self.curs.execute(first_query)
                res_fst_query = self.curs.fetchall()

                if not len(res_fst_query) == 0:
                    second_query = f"SELECT fw.id, fw.modified " \
                                   f"FROM content.film_work fw " \
                                   f"LEFT JOIN content.{table}_film_work tb ON tb.film_work_id = fw.id " \
                                   f"WHERE tb.{table}_id IN {get_ids(res_fst_query)} "
                    self.curs.execute(second_query)
                    res_snd_query = self.curs.fetchall()

                    query = f"SELECT fw.id, fw.title, fw.description, fw.rating, " \
                            f"ARRAY_AGG(DISTINCT content.genre.name::text) AS genres, " \
                            f"ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', " \
                            f"content.person.full_name)) FILTER (WHERE pfw.role = 'director') AS directors, " \
                            f"ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', " \
                            f"content.person.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors, " \
                            f"ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', " \
                            f"content.person.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers, " \
                            f"MAX({table}.modified) AS tab_modif " \
                            f"FROM content.film_work fw " \
                            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id " \
                            f"LEFT JOIN content.person ON content.person.id = pfw.person_id " \
                            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id " \
                            f"LEFT JOIN content.genre ON content.genre.id = gfw.genre_id " \
                            f"WHERE fw.id IN {get_ids(res_snd_query)} " \
                            f"GROUP BY fw.id ORDER BY tab_modif"
                else:
                    return []
        self.curs.execute(query)
        return self.curs.fetchall()
