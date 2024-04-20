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
from transform import transform_data


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

    dt = '2020-06-16 23:14:09.320625+03:00'
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
        trd_query = psql_data.extract_data('film_work', dt)
        # Если индекс осутсвует, то создаем
        if not es_conn.indices.exists(index=os.environ.get('ES_INDEX')):
            create_idx_res = ElasticLoad(es_conn, os.environ.get('ES_INDEX')).create_idx()
            logging.info(create_idx_res)

        # Преобразовываем данные для загрузки в ES
        data_to_load = transform_data(trd_query)
        for data in data_to_load:
            logging.info(data)

        # Загружаем преобразованные данные
        save_es_res = ElasticLoad(es_conn, os.environ.get('ES_INDEX')).save_data(data_to_load)
        logging.info(save_es_res)

        # ElasticLoad(es_conn, "test_fw").get_data()


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

    # dt = datetime.datetime(2021, 6, 16, 23, 14, 9, 320625,
    #                       tzinfo=datetime.timezone(datetime.timedelta(seconds=10800)))
    main()

    logging.info("Работа программы завершена!")
