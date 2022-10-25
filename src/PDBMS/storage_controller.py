import os
from typing import List, Dict

import pandas as pd


class StorageController:
    @staticmethod
    def __is_directory_exists(path: str) -> bool:
        return os.path.exists(path)

    @staticmethod
    def create_directory(path: str) -> None:
        if not StorageController.__is_directory_exists(path):
            os.mkdir(path)

    @staticmethod
    def get_directories_list(path: str) -> List[str]:
        return os.listdir(path)

    @staticmethod
    def get_base_name(path: str) -> str:
        return os.path.basename(path)

    @staticmethod
    def get_new_path(path: str, file_name: str) -> str:
        return os.path.join(path, file_name)

    @staticmethod
    def get_columns_names(path: str) -> List[str]:
        columns = []
        if StorageController.__is_directory_exists(path):
            empty_df_with_header = pd.read_csv(path, sep='\t')
            columns = list(empty_df_with_header.columns.values)
        return columns

    @staticmethod
    def fill_table_header(path: str, columns_names: List[str]) -> List[str]:
        empty_table_with_header = pd.DataFrame(columns=columns_names)
        empty_table_with_header.to_csv(path, sep='\t', index=False)
        return StorageController.get_columns_names(path)

    @staticmethod
    def insert_data(path: str, record: Dict) -> None:
        df = pd.DataFrame(record, columns=list(record.keys()))
        df.to_csv(path, sep='\t', index=False)

    @staticmethod
    def select_data(path: str, query_options: Dict) -> pd.DataFrame:
        df = pd.read_csv(path, sep='\t')
        selected_df = pd.DataFrame()
        for key, value in query_options.items():
            selected_rows = df[(df[key].isin(value))]
            selected_df = pd.concat([selected_df, selected_rows], ignore_index=True).drop_duplicates().\
                reset_index(drop=True)

        return selected_df

    @staticmethod
    def delete(path: str, query_options: Dict) -> None:
        df = pd.read_csv(path, sep='\t')
        for key, value in query_options.items():
            selected_df = df[df[key].isin(value)].index
            df.drop(selected_df, inplace=True)
