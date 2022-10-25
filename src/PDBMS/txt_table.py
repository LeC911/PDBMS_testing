import logging
from typing import List, Dict, TypeVar

import pandas as pd

from src.PDBMS.storage_controller import StorageController

T = TypeVar('T')


class TxtTable:

    def __init__(self, path: str):
        self._path = path
        self._name = StorageController.get_base_name(self._path)
        self._fields = self.__read_fields()

    def __read_fields(self) -> List[str]:
        fields = StorageController.get_columns_names(self._path)
        return fields

    @property
    def get_name(self) -> str:
        return self._name

    @property
    def get_fields(self) -> List[str]:
        return self._fields

    @property
    def get_path(self) -> str:
        return self._path

    def fill_table(self, columns_names: List[str]) -> None:
        self._fields = StorageController.fill_table_header(self._path, columns_names)

    def insert(self, records: Dict) -> None:
        self.__check_records_type(records, TxtTable.insert.__qualname__)
        self.__validate_values(records, TxtTable.insert.__qualname__)
        StorageController.insert_data(self._path, records)

    def __validate_values(self, values_dict: Dict, method_name: str) -> None:
        for key in values_dict.keys():
            if key not in self._fields:
                logging.warning(f"In {method_name}():"
                                f"Unknown field name '{key}' in inserted data")
                raise ValueError()

    def __check_records_type(self, records: T, method_name: str) -> None:
        if type(records) is not dict:
            logging.warning(f"In {method_name}(): Inserted data should be dictionary,"
                            f"but there is {type(records)}")
            raise TypeError()

    def select(self, query_options: Dict) -> pd.DataFrame:
        self.__validate_options(query_options, TxtTable.select.__qualname__)
        return StorageController.select_data(self._path, query_options)

    def delete(self, query_options: Dict) -> None:
        self.__validate_options(query_options, TxtTable.delete.__qualname__)
        StorageController.delete(self._path, query_options)

    def __validate_options(self, query_options: Dict, method_name: str) -> None:
        for key in query_options.keys():
            if key not in self._fields:
                logging.warning(f"In {method_name}():"
                                f"Unknown field name '{key}' in query options")
                raise ValueError()
