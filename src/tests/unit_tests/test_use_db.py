import os
import shutil
import unittest

from src.PDBMS.pdbms import Pdbms


class TestUseDB(unittest.TestCase):
    def setUp(self) -> None:
        self.db_patch = "testDB"

    def tearDown(self) -> None:
        if os.path.exists(self.db_patch):
            shutil.rmtree(self.db_patch)

    def test_use_existing_database(self):
        db_name = "test_db2"
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertEqual(pdbms.get_active_db().get_name, db_name)

    def test_use_existing_database(self):
        db_name = "test_db2"
        pdbms = Pdbms(self.db_patch)

        pdbms.create_db(db_name)
        pdbms.use_db(db_name)

        self.assertEqual(pdbms.get_active_db().get_name, db_name)

    def test_use_non_existent_database(self):
        db_name = "test_db4"
        pdbms = Pdbms(self.db_patch)

        self.assertRaises(FileNotFoundError, pdbms.use_db, db_name)
