import os
import shutil
import unittest

from src.PDBMS.custom_exception import EmptyNameError
from src.PDBMS.pdbms import Pdbms


class TestCreateDB(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_create_database(self):
        db_name = "test_db1"
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)

        self.assertEqual(pdbms.get_databases_names()[0], db_name)

    def test_create_database_with_existing_name(self):
        db_name = "test_db1"
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)

        self.assertRaises(FileExistsError, pdbms.create_db, db_name)

    def test_create_database_with_empty_name(self):
        db_name = ""
        pdbms = Pdbms(self.db_patch)

        self.assertRaises(EmptyNameError, pdbms.create_db, db_name)
