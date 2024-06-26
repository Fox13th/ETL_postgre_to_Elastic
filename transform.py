import logging

from pydantic import BaseModel


class DataMovies(BaseModel):
    id: str
    title: str
    description: str
    imdb_rating: float
    directors: list
    actors: list
    writers: list
    directors_names: list
    actors_names: list
    writers_names: list
    genres: list


def get_names(names: list):
    names_list = []
    if names is not None:
        for name in names:
            names_list.append(name['name'])
    return names_list


def transform_data(data_to_trans):
    """
    Преобразование данных в для загрузки в ElasticSearch
    :param data_to_trans: Данные для преобразования
    :return: Преобразованные данные
    """
    list_data_for_es = []

    for data in data_to_trans:
        dict_data = {'id': data[0],
                     'title': data[1],
                     'description': data[2],
                     'imdb_rating': data[3],
                     'directors': data[5],
                     'actors': data[6],
                     'writers': data[7],
                     'genres': data[4],
                     'directors_names': get_names(data[5]),
                     'actors_names': get_names(data[6]),
                     'writers_names': get_names(data[7])
                     }

        try:
            DataMovies(**dict_data)
        except ValueError as err:
            error = err.errors()
            for val_err in error:
                match val_err['type']:
                    case 'string_type':
                        if not dict_data[val_err['loc'][0]] is None:
                            logging.error(f"Type of '{val_err['loc'][0]}' is invalid. It's not a string type.")
                            exit()
                    case 'float_parsing':
                        logging.error(f"Type of '{val_err['loc'][0]}' is invalid. It's not a float type.")
                        exit()
                    case 'list_type':
                        if not dict_data[val_err['loc'][0]] is None:
                            logging.error(f"Type of '{val_err['loc'][0]}' is invalid. It's not a list type.")
                            exit()
        finally:
            list_data_for_es.append(dict_data)
    return list_data_for_es
