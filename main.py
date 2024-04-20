import time
import logging
import os
from contextlib import closing
import elastic_transport
import psycopg2
from psycopg2.extras import DictCursor
from psql_exctractor import PSQLExtractor
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elastic_load import ElasticLoad
from state_storage import JsonFileStorage


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        def inner(*args, **kwargs):
            iteration = 0
            curr_sleep_time = start_sleep_time

            while True:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, elastic_transport.ConnectionError) as error:
                    if curr_sleep_time < border_sleep_time:
                        curr_sleep_time = start_sleep_time * factor ** iteration

                    if curr_sleep_time > border_sleep_time:
                        curr_sleep_time = border_sleep_time

                    logging.error(f"Остуствует соединение. Повторное подключение через {curr_sleep_time} сек.")
                    logging.error(error)

                    time.sleep(curr_sleep_time)
                    iteration += 1

        return inner

    return func_wrapper


@backoff(1, 2, 100)
def main():
    # Загрузка состояния по полю modified
    file_state_path = os.environ.get('STATE_FILE')
    if not os.path.exists(file_state_path):
        JsonFileStorage(file_state_path).save_state({'modified': os.environ.get('START_DATE')})
    load_state = JsonFileStorage(file_state_path).retrieve_state()

    with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn, \
            closing(Elasticsearch("http://localhost:9200/", max_retries=0)) as es_conn:
        psql_data = PSQLExtractor(pg_conn)

        # Для запрос по Persons=================================
        # fst_query = psql_data.extract_data('first', dt)
        # id_person = get_ids(fst_query)
        # snd_query = psql_data.extract_data('second', id_person)
        # id_film = get_ids(snd_query)
        # trd_query = psql_data.extract_data('third', id_film)
        # =======================================================
        # Запрос кино
        try:
            # Проверка на существования ключа и походящего типа
            dt = load_state['modified']
        except KeyError:
            logging.error(f"Остуствие ключа 'modified' => в качестве начального значения берем по умолчанию")
            load_state['modified'] = os.environ.get('START_DATE')
        finally:
            trd_query = psql_data.extract_data('film_work', dt)
            # ===========================================================
            # --------------------ElasticSearch--------------------------
            # ===========================================================
            client_db = ElasticLoad(es_conn, os.environ.get('ES_INDEX'))
            # Если индекс осутсвует, то создаем
            if not es_conn.indices.exists(index=os.environ.get('ES_INDEX')):
                create_idx_res = client_db.create_idx()
                logging.info(create_idx_res)

            logging.info(f"Загрузка данных начиная с {dt}")
            client_db.save_data(trd_query, file_state_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
    logging.info("Начало работы программы")

    load_dotenv()

    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('HOST'),
        'port': os.environ.get('PORT'),
    }

    main()

    logging.info("Работа программы завершена!")
