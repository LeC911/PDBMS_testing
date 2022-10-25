import unittest
from unittest.mock import MagicMock

from src.tests.integration_tests.abstract_integration_test import AbstractIntegrationTest
from src.PDBMS.storage_controller import StorageController


class IntegrationTestSelectInsertDelete(unittest.TestCase, AbstractIntegrationTest):
    @classmethod
    def setUpClass(cls) -> None:
        cls.set_up()
        cls.pdbms.create_db(cls.db_name)
        cls.pdbms.use_db(cls.db_name)
        cls.pdbms.create_table(cls.table_name, cls.columns_names)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tear_down()

    def test_select_data_on_call_select(self):
        StorageController.select_data = MagicMock()
        self.pdbms.select(self.table_name, {}), True  # TODO: read!!!
        StorageController.select_data.assert_called_once()

    def test_insert_data_on_call_insert(self):
        StorageController.insert_data = MagicMock()
        self.pdbms.insert(self.table_name, self.data)
        StorageController.insert_data.assert_called_once()

    def test_delete_data_on_call_delete(self):
        StorageController.delete = MagicMock()
        self.pdbms.delete(self.table_name, {})
        StorageController.delete.assert_called_once()


if __name__ == "__main__":
    unittest.main()