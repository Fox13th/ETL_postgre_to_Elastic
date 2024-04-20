import logging

import psycopg2


def get_ids(pack_data):
    return tuple([id_pers[0] for id_pers in pack_data])


class PSQLExtractor:

    def __init__(self, connection):
        self.conn = connection
        self.curs = self.conn.cursor()
        self.log = logging.getLogger(self.__class__.__name__)

    def extract_data(self, query, data):
        """
        Данная функция осуществляет запросы в PostgreSQL для получения данных о кинопроизведениях PERSONS
        :param query: какой запрос выполнить: first, second, third
        :param data: по какому критерию осуществлять отбор
        :return: полученные данные запроса в виде списка
        """
        try:
            match query:
                case 'first':
                    psql_query = f"SELECT id, modified FROM content.person WHERE modified > '{data}' ORDER BY " \
                                 f"modified LIMIT 100 "
                case 'second':
                    psql_query = f"SELECT fw.id, fw.modified FROM content.film_work fw LEFT JOIN " \
                                 f"content.person_film_work pfw ON pfw.film_work_id = fw.id WHERE pfw.person_id IN " \
                                 f"{data} ORDER BY fw.modified LIMIT 100 "
                case 'third':
                    psql_query = f"SELECT fw.id as fw_id, fw.title, fw.description, fw.rating, fw.type, fw.created, " \
                                 f"fw.modified, pfw.role, p.id, p.full_name, g.name FROM content.film_work fw LEFT " \
                                 f"JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id LEFT JOIN " \
                                 f"content.person p ON p.id = pfw.person_id LEFT JOIN content.genre_film_work gfw ON " \
                                 f"gfw.film_work_id = fw.id LEFT JOIN content.genre g ON g.id = gfw.genre_id WHERE " \
                                 f"fw.id IN {data} "

                case 'film_work':
                    psql_query = f"SELECT fw.id, fw.title, fw.description, fw.rating, " \
                                 f"ARRAY_AGG(DISTINCT jsonb_build_object('name', genre.name)) AS genres, " \
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
            self.curs.execute(psql_query)
            res_query = self.curs.fetchall()
            #raise TypeError("Возникла ошибка типа поля 'modified'")
        except Exception as err:
            self.log.error(err)
            exit()
        return res_query