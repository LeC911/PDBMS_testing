import logging
from typing import Dict, List

import pandas as pd

from src.PDBMS.custom_exception import EmptyNameError
from src.PDBMS.txt_db import TxtDB
from src.PDBMS.storage_controller import StorageController


class Pdbms:

    def __init__(self, path: str):
        self._path = path
        StorageController.create_directory(self._path)
        self._databases = self.__scan_for_databases()
        self._active_db = -1

        logging.basicConfig(filename="pdbms.log", level=logging.WARNING, filemode="w")

    def __scan_for_databases(self) -> List[TxtDB]:
        database_names = []
        dir_names = StorageController.get_directories_list(self._path)
        for name in dir_names:
            database_names.append(TxtDB(StorageController.get_new_path(self._path, name)))
        return database_names

    def get_active_db(self) -> TxtDB:
        if self._active_db == -1:
            logging.warning(f"In {Pdbms.get_active_db.__qualname__}(): No database selected")
            raise RuntimeError()
        return self._databases[self._active_db]

    def create_db(self, db_name: str) -> None:
        self.__check_db_existence(db_name, Pdbms.create_db.__qualname__)
        self.__check_db_name_for_emptiness(db_name)
        self._databases.append(TxtDB(StorageController.get_new_path(self._path, db_name)))

    def __check_db_existence(self, db_name: str, method_name: str) -> None:
        for db in self._databases:
            if db.get_name == db_name:
                logging.warning(f"In {method_name}(): Database '{db_name}'"
                                f"already exists in the '{self._path}' directory")
                raise FileExistsError()

    def __check_db_name_for_emptiness(self, db_name: str) -> None:
        if not db_name:
            logging.warning(f"In {Pdbms.create_db.__qualname__}(): "
                            f"The name of the object '{type(self)}' is an empty string")
            raise EmptyNameError(self)

    def get_databases_names(self) -> List[str]:
        return [db.get_name for db in self._databases]

    def get_tables_names(self) -> List[str]:
        return self.get_active_db().get_tables_names

    def get_table_fields(self, table_name: str) -> List[str]:
        return self.get_active_db().get_table_fields(table_name)

    def use_db(self, db_name: str) -> None:
        is_there_db = False

        for i in range(len(self._databases)):
            if self._databases[i].get_name == db_name:
                self._active_db = i
                is_there_db = True
                break

        if not is_there_db:
            logging.warning(f"In {Pdbms.use_db.__qualname__}(): Database '{db_name}'"
                            f"not found in '{self._path}' directory")
            raise FileNotFoundError()

    def create_table(self, table_name: str, columns_names: List[str]) -> None:
        self.get_active_db().create_table(table_name, columns_names)

    def insert(self, table_name: str, records: Dict):
        self.get_active_db().insert(table_name, records)

    def select(self, table_name: str, query_options: Dict) -> pd.DataFrame:
        return self.get_active_db().select(table_name, query_options)

    def delete(self, table_name: str, query_options: Dict) -> None:
        return self.get_active_db().delete(table_name, query_options)
