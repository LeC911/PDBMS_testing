import os
import shutil
import unittest
import pandas as pd

from src.PDBMS.pdbms import Pdbms


class TestDelete(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"
        self.columns_names = ["Name", "Age", "Work"]

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_delete_data_from_table_with_correct_single_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ross", "Chandler", "Joey", "Rachel", "Phoebe", "Monica"],
            self.columns_names[1]: [28, 26, 26, 26, 26, 26],
            self.columns_names[2]: ["Paleontology", "Office", "Acting", "Service", "Health", "Cooking"]
        }
        query_options_for_delete = {
            self.columns_names[1]: [26]
        }
        query_options_for_select_after_deletion = {
            self.columns_names[1]: [28]
        }
        result_selection = pd.DataFrame(
            {
                "Name": ["Ross"],
                "Age": [28],
                "Work": ["Paleontology"]
            }, columns=self.columns_names)

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)
        pdbms.delete(table_name, query_options_for_delete)

        self.assertEqual(pdbms.select(table_name, query_options_for_select_after_deletion).shape[0],
                         result_selection.shape[0])
        self.assertTrue(
            result_selection.compare(pdbms.select(table_name, query_options_for_select_after_deletion)).empty)

    def test_delete_data_from_table_with_correct_multiple_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ann", "Jack", "Michael", "Jim", "Sam"],
            self.columns_names[1]: [18, 34, 23, 26, 26],
            self.columns_names[2]: ["Math", "Science", "Programming", "Science", "Programming"]
        }
        query_options_for_delete = {
            self.columns_names[2]: ["Programming"],
            self.columns_names[1]: [26]
        }
        query_options_for_select_after_deletion = {
            self.columns_names[1]: [18, 34]
        }
        result_selection = pd.DataFrame(
            {
                "Name": ["Ann", "Jack"],
                "Age": [18, 34],
                "Work": ["Math", "Science"]
            }, columns=self.columns_names)

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)
        pdbms.delete(table_name, query_options_for_delete)

        self.assertEqual(pdbms.select(table_name, query_options_for_select_after_deletion).shape[0],
                         result_selection.shape[0])
        self.assertTrue(
            result_selection.compare(pdbms.select(table_name, query_options_for_select_after_deletion)).empty)

    def test_delete_data_from_table_with_empty_query_options(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        data = {
            self.columns_names[0]: ["Ann", "Jack", "Michael"],
            self.columns_names[1]: [18, 34, 25],
            self.columns_names[2]: ["Math", "Science", "Programming"]
        }
        query_options_for_delete = {}
        query_options_for_select_after_deletion = {
            self.columns_names[1]: [18, 34, 25]
        }
        result_selection_after_deletion = pd.DataFrame(data, columns=self.columns_names)

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)
        pdbms.delete(table_name, query_options_for_delete)

        self.assertEqual(pdbms.select(table_name, query_options_for_select_after_deletion).shape[0],
                         result_selection_after_deletion.shape[0])
        self.assertTrue(result_selection_after_deletion.
                        compare(pdbms.select(table_name, query_options_for_select_after_deletion)).empty)

    def test_delete_data_from_non_existent_table(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        query_options = {
            self.columns_names[1]: [26]
        }

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertRaises(FileNotFoundError, pdbms.delete, table_name, query_options)

    def test_delete_data_from_table_with_invalid_columns(self):
        db_name = "test_db1"
        table_name = "test_table1.csv"
        query_options = {
            "Count": [26]
        }

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)

        self.assertRaises(ValueError, pdbms.delete, table_name, query_options)
