import logging
from elasticsearch.helpers import streaming_bulk
from state_storage import JsonFileStorage
import tqdm
from transform import transform_data


class ElasticLoad:

    def __init__(self, connection, name_index):
        self.conn = connection
        self.name_idx = name_index
        self.log = logging.getLogger(self.__class__.__name__)

    def save_data(self, res_query, file_state: str):
        """
        Данная функция осуществляет загрузку данных в ElasticSearch
        :param res_query: Результат запроса, который подается на вход обработчику данных для загрузки в ES
        :param file_state: JSON с состоянием загрузки
        """
        docs = transform_data(res_query)
        logging.info("Полученные с PostgreSQL данные успешно обработаны!")

        number_of_docs = len(docs)
        progress = tqdm.tqdm(unit=" docs", total=number_of_docs)
        successes = 0
        for ok, action in streaming_bulk(
                client=self.conn, index=self.name_idx, actions=self.generate_data(docs),
        ):
            progress.update(1)
            print(res_query[successes][8])
            JsonFileStorage(file_state).save_state({'modified': str(res_query[successes][8])})
            successes += ok

    def generate_data(self, docs: list):
        for doc in docs:
            doc['_index'] = self.name_idx
            doc['_id'] = doc['id']
            yield doc

    def create_idx(self):
        """
        Создание индекса по заранее опредленной структуре
        :return:
        """
        settings = {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        }
        mappings = {
            "dynamic": "strict",
            "properties": {
                "id": {"type": "keyword"},
                "imdb_rating": {"type": "float"},
                "genres": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "directors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "writers_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "directors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                }
            }
        }

        resp = self.conn.indices.create(index=self.name_idx, settings=settings, mappings=mappings)
        return resp
