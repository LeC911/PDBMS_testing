from typing import List, Dict
import logging

import pandas as pd

from src.PDBMS.custom_exception import EmptyNameError
from src.PDBMS.txt_table import TxtTable
from src.PDBMS.storage_controller import StorageController


class TxtDB:

    def __init__(self, path: str):
        self._path = path
        self._name = StorageController.get_base_name(self._path)
        StorageController.create_directory(self._path)
        self._tables = self.__scan_for_tables()

    @property
    def get_name(self) -> str:
        return self._name

    @property
    def get_tables_names(self) -> List[str]:
        return [table.get_name for table in self._tables]

    def __scan_for_tables(self) -> List[TxtTable]:
        dir_names = StorageController.get_directories_list(self._path)
        table_names = [TxtTable(StorageController.get_new_path(self._path, name)) for name in dir_names]
        return table_names

    def create_table(self, table_name: str, columns_names: List[str]) -> None:
        table = self.__get_table_with_specific_name(table_name)
        if table is not None:
            logging.warning(f"In {TxtDB.create_table.__qualname__}(): Table '{table_name}' already exists")
            raise FileExistsError()
        self.__check_table_name_for_emptiness(table_name)
        table = TxtTable(StorageController.get_new_path(self._path, table_name))
        self._tables.append(table)
        table.fill_table(columns_names)

    def __check_table_name_for_emptiness(self, table_name: str) -> None:
        if not table_name:
            logging.warning(f"In {TxtDB.create_table.__qualname__}(): "
                            f"The name of the object '{type(self)}' is an empty string")
            raise EmptyNameError(self)

    def get_table_fields(self, table_name: str) -> List[str]:
        table = self.__check_table_existence(table_name, TxtDB.get_table_fields.__qualname__)
        return table.get_fields

    def insert(self, table_name: str, records: Dict) -> None:
        table = self.__check_table_existence(table_name, TxtDB.insert.__qualname__)
        table.insert(records)

    def select(self, table_name: str, query_options: Dict) -> pd.DataFrame:
        table = self.__check_table_existence(table_name, TxtDB.select.__qualname__)
        selected_values = table.select(query_options)
        return selected_values

    def delete(self, table_name: str, query_options: Dict) -> None:
        table = self.__check_table_existence(table_name, TxtDB.delete.__qualname__)
        table.delete(query_options)

    def __get_table_with_specific_name(self, table_name: str) -> TxtTable:
        return next((table for table in self._tables if table.get_name == table_name), None)

    def __check_table_existence(self, table_name: str, method_name: str) -> TxtTable:
        table = self.__get_table_with_specific_name(table_name)
        if table is None:
            logging.warning(f"In {method_name}(): Table '{table_name}' not found")
            raise FileNotFoundError()
        return table
