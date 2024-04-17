import logging
from elasticsearch.helpers import bulk


class ElasticLoad:

    def __init__(self, connection, name_index):
        self.conn = connection
        self.name_idx = name_index
        self.log = logging.getLogger(self.__class__.__name__)

    def create_idx(self):
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

    def save_data(self, docs):
        """
        Данная функция осуществляет загрузку данных в ElasticSearch
        :param docs:
        :param id_idx: ID документа
        :param name_idx: Название индекса
        :param doc: Данные на загрузку
        :return:
        """
        return bulk(self.conn, self.gendata(docs))  # self.conn.index(index=self.name_idx, id=id_idx, document=doc)

    def gendata(self, docs: list):
        for doc in docs:
            doc['_index'] = self.name_idx
            doc['_id'] = doc['id']
            yield doc

    def get_data(self):
        """
        Данная функция осуществляет получение данных по названию индекса и id документа
        :param name_idx: Имя индекса
        :param doc_id: ID документа
        :return:
        """
        resp = self.conn.get(index=self.name_idx, id=1)
        print(resp["_source"])
        self.conn.indices.refresh(index=self.name_idx)
