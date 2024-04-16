import logging
from pydantic import BaseModel
from elasticsearch.helpers import bulk


class DataMovies(BaseModel):
    id: str
    imdb_rating: float
    #genres: str => list
    title: str
    description: str
    role: str
    id_pers: str
    full_name: str


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

    def transform_data(self, data_to_trans):
        """
        Преобразование данных в для загрузки в ElasticSearch
        :param data_to_trans: Данные для преобразования
        :return: Преобразованные данные
        """
        list_data_for_es = []
        for data in data_to_trans:
            dict_data = {
                'id': data[0],
                'title': data[1],
                'description': data[2],
                'imdb_rating': data[3],
                'directors': data[5],
                'actors': data[6],
                'writers': data[7]
            }

            genre_str = ""
            for genre in data[4]:
                genre_str += genre['name'] + ", "
            dict_data['genres'] = genre_str[:-2]



            try:
                DataMovies(**dict_data)
            except ValueError as err:
                error = err.errors()
                for val_err in error:
                    match val_err['type']:
                        case 'string_type':
                            if not dict_data[val_err['loc'][0]] is None:
                                raise ValueError(f"Type of '{val_err['loc'][0]}' is invalid. It's not a string type.")
                        case 'float_parsing':
                            raise ValueError(f"Type of '{val_err['loc'][0]}' is invalid. It's not a float type.")
            finally:
                list_data_for_es.append(dict_data)
        return list_data_for_es

    def save_data(self, docs):#id_idx, doc: dict):
        """
        Данная функция осуществляет загрузку данных в ElasticSearch
        :param docs:
        :param id_idx: ID документа
        :param name_idx: Название индекса
        :param doc: Данные на загрузку
        :return:
        """
        return bulk(self.conn, self.gendata(docs)) #self.conn.index(index=self.name_idx, id=id_idx, document=doc)

    def gendata(self, docs: list):
        for doc in docs:
            doc['_index'] = self.name_idx
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
