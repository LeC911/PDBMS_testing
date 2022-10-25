import os
import shutil
import unittest

from src.PDBMS.custom_exception import EmptyNameError
from src.PDBMS.pdbms import Pdbms


class TestCreateTable(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_create_table_in_existing_and_selected_database(self):
        db_name = "test_db5"
        table_name = "test_table1.csv"
        columns_names = ["Name", "Age", "Work"]
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, columns_names)

        self.assertEqual(pdbms.get_tables_names()[0], table_name)
        for i in range(len(columns_names)):
            self.assertEqual(pdbms.get_table_fields(table_name)[i], columns_names[i])

    def test_create_table_with_existing_name(self):
        db_name = "test_db5"
        table_name = "test_table1.csv"
        columns_names = ["Name", "Age"]
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)  # TODO: many acts?
        pdbms.use_db(db_name)
        pdbms.create_table(table_name, columns_names)

        self.assertRaises(FileExistsError, pdbms.create_table, table_name, columns_names)

    def test_create_table_in_inactive_database(self):
        db_name = "test_db6"
        table_name = "test_table2.csv"
        columns_names = ["Name", "Age", "Work"]
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)

        self.assertRaises(RuntimeError, pdbms.create_table, table_name, columns_names)

    def test_create_table_with_empty_name(self):
        db_name = "test_db6"
        table_name = ""
        columns_names = ["Name", "Age", "Work"]
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertRaises(EmptyNameError, pdbms.create_table, table_name, columns_names)