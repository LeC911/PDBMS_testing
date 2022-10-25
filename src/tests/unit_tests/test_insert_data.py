import os
import shutil
import unittest

import pandas as pd

from src.PDBMS.pdbms import Pdbms


class TestInsert(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"
        self.columns_names = ["Name", "Age", "Work"]

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_insert_correct_data(self):
        db_name = "test_db7"
        table_name = "test_table3.csv"
        data = {
            self.columns_names[0]: ["Ann", "Joe"],
            self.columns_names[1]: [18, 25],
            self.columns_names[2]: ["Service", "Office"]
        }
        result_selection = pd.DataFrame(data, columns=self.columns_names)
        query_options = {
            self.columns_names[1]: [18, 25]
        }

        pdbms = Pdbms(self.db_patch)
        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)
        pdbms.insert(table_name, data)

        self.assertEqual(pdbms.select(table_name, query_options).shape[0], result_selection.shape[0])
        self.assertTrue(result_selection.compare(pdbms.select(table_name, query_options)).empty)

    def test_insert_data_into_non_existent_table(self):
        db_name = "test_db9"
        table_name = "test_table5.csv"
        data = {
            self.columns_names[0]: ["test_value1"],
            self.columns_names[1]: ["test_value2"]
        }
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertRaises(FileNotFoundError, pdbms.insert, table_name, data)

    def test_insert_data_with_wrong_type(self):
        db_name = "test_db10"
        table_name = "test_table5.csv"
        data = ("test_value1", "test_value2")
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)

        self.assertRaises(TypeError, pdbms.insert, table_name, data)

    def test_insert_data_with_wrong_fields(self):
        db_name = "test_db11"
        table_name = "test_table5.csv"
        data = {
            "test_col3": ["test_value1"],
            "test_col4": ["test_value2"]
        }
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, self.columns_names)

        self.assertRaises(ValueError, pdbms.insert, table_name, data)
