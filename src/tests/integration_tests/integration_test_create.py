import os
import unittest
from unittest.mock import MagicMock

from src.tests.integration_tests.abstract_integration_test import AbstractIntegrationTest
from src.PDBMS.storage_controller import StorageController


class IntegrationTestCreate(unittest.TestCase, AbstractIntegrationTest):
    @classmethod
    def setUpClass(cls) -> None:
        cls.set_up()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tear_down()

    def test_create_directory_on_call_create_db(self):
        StorageController.create_directory = MagicMock()
        self.pdbms.create_db(self.db_name)
        StorageController.create_directory.assert_called_once()


if __name__ == "__main__":
    unittest.main()
