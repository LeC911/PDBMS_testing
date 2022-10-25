import unittest

from src.tests.unit_tests.test_use_db import TestUseDB
from src.tests.unit_tests.test_create_db import TestCreateDB
from src.tests.unit_tests.test_use_db import TestUseDB
from src.tests.unit_tests.test_create_table import TestCreateTable
from src.tests.unit_tests.test_select_data import TestSelect
from src.tests.unit_tests.test_insert_data import TestInsert
from src.tests.unit_tests.test_delete_data import TestDelete


def run_unit_tests() -> None:
    test_classes_to_run = [TestUseDB, TestDelete, TestSelect, TestInsert,
                           TestCreateTable, TestUseDB, TestCreateDB]
    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(big_suite)


if __name__ == '__main__':
    run_unit_tests()
