import shutil
import os
from abc import ABC

from src.PDBMS.pdbms import Pdbms


class AbstractIntegrationTest(ABC):
    @classmethod
    def set_up(cls) -> None:
        cls.db_patch = "testDB"
        cls.db_name = "test_db"
        cls.table_name = "test_table"
        cls.columns_names = ["Name", "Age", "Work"]
        cls.data = {
            cls.columns_names[0]: ["Ann", "Jack", "Michael"],
            cls.columns_names[1]: [18, 34, 25],
            cls.columns_names[2]: ["Math", "Science", "Programming"]
        }

        cls.pdbms = Pdbms(cls.db_patch)

        if not os.path.exists(os.path.join(cls.db_patch, cls.db_name)):
            os.mkdir(os.path.join(cls.db_patch, cls.db_name))

    @classmethod
    def tear_down(cls) -> None:
        if os.path.exists(cls.db_patch):
            shutil.rmtree(cls.db_patch)
