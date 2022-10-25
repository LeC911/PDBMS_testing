import os
import shutil
import unittest
import pandas as pd

from src.PDBMS.pdbms import Pdbms


class TestSelect(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"
        self.columns_names = ["Name", "Age", "Work"]

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_select_data_from_table_with_single_correct_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ann", "Jack", "Michael"],
            self.columns_names[1]: [18, 34, 25],
            self.columns_names[2]: ["Math", "Science", "Programming"]
        }
        query_options = {
            self.columns_names[2]: ["Math", "Science"]
        }
        result_selection = pd.DataFrame(
            {
                self.columns_names[0]: ["Ann", "Jack"],
                self.columns_names[1]: [18, 34],
                self.columns_names[2]: ["Math", "Science"]
            }, columns=self.columns_names)

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)

        self.assertEqual(pdbms.select(table_name, query_options).shape[0], result_selection.shape[0])
        self.assertTrue(result_selection.compare(pdbms.select(table_name, query_options)).empty)

    def test_select_data_from_table_with_multiple_correct_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ann", "Jack", "Michael", "Jim", "Sam"],
            self.columns_names[1]: [18, 34, 23, 26, 26],
            self.columns_names[2]: ["Math", "Science", "Programming", "Science", "Programming"]
        }
        query_options = {
            self.columns_names[2]: ["Math", "Science"],
            self.columns_names[1]: [26]
        }
        result_selection = pd.DataFrame(
            {
                self.columns_names[0]: ["Ann", "Jack", "Jim", "Sam"],
                self.columns_names[1]: [18, 34, 26, 26],
                self.columns_names[2]: ["Math", "Science", "Science", "Programming"]
            }, columns=self.columns_names)

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)

        self.assertEqual(pdbms.select(table_name, query_options).shape[0], result_selection.shape[0])
        self.assertTrue(result_selection.compare(pdbms.select(table_name, query_options)).empty)

    def test_select_data_from_table_with_empty_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ann", "Jack", "Michael"],
            self.columns_names[1]: [18, 34, 25],
            self.columns_names[2]: ["Math", "Science", "Programming"]
        }
        query_options = {}

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)

        self.assertTrue(pdbms.select(table_name, query_options).empty)

    def test_select_data_from_non_existent_table(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        query_options = {
            self.columns_names[2]: ["Math", "Science"]
        }

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertRaises(FileNotFoundError, pdbms.select, table_name, query_options)

    def test_select_data_from_table_with_invalid_columns(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        query_options = {
            "Count": ["Math", "Science"]
        }

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)

        self.assertRaises(ValueError, pdbms.select, table_name, query_options)