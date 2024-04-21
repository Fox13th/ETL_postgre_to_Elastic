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
from state_storage import State, JsonFileStorage


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

    fw_load_state = State(JsonFileStorage(file_state_path_fw)).get_state('modified')
    g_load_state = State(JsonFileStorage(file_state_path_g)).get_state('modified')
    p_load_state = State(JsonFileStorage(file_state_path_p)).get_state('modified')

    with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn, \
            closing(Elasticsearch("http://localhost:9200/", max_retries=0)) as es_conn:
        psql_data = PSQLExtractor(pg_conn)

        for table in ["film_work", "genre", "person"]:
            match table:
                case "film_work":
                    dt = fw_load_state
                    file_state = file_state_path_fw
                case "genre":
                    dt = g_load_state
                    file_state = file_state_path_g
                case "person":
                    dt = p_load_state
                    file_state = file_state_path_p

            query = psql_data.init_query(table, dt)

            if not len(query) == 0:
                client_db = ElasticLoad(es_conn, os.environ.get('ES_INDEX'))

                # Если индекс осутсвует, то создаем
                if not es_conn.indices.exists(index=os.environ.get('ES_INDEX')):
                    create_idx_res = client_db.create_idx()
                    logging.info(create_idx_res)

                logging.info(f"Загрузка данных в {table} начиная с {dt}")
                client_db.save_data(query, file_state)
                logging.info(f"Данные успешно загружены в {table}")


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

    # Загрузка состояния по полю modified
    file_state_path_fw = os.environ.get('STATE_FILE_FW')
    file_state_path_g = os.environ.get('STATE_FILE_G')
    file_state_path_p = os.environ.get('STATE_FILE_P')

    if not os.path.exists(file_state_path_fw):
        State(JsonFileStorage(file_state_path_fw)).set_state('modified', os.environ.get('START_DATE'))
    if not os.path.exists(file_state_path_g):
        State(JsonFileStorage(file_state_path_g)).set_state('modified', os.environ.get('START_DATE'))
    if not os.path.exists(file_state_path_p):
        State(JsonFileStorage(file_state_path_p)).set_state('modified', os.environ.get('START_DATE'))

    try:
        while 1:

            main()
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Работа программы завершена!")
