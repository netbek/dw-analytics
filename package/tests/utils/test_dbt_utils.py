from package.utils.dbt_utils import extract_table_name

import unittest


class TestExtractTableName(unittest.TestCase):
    def test_ref(self):
        self.assertEqual(extract_table_name("ref('table_name')"), "table_name")

    def test_source(self):
        self.assertEqual(extract_table_name("source('source_name', 'table_name')"), "table_name")
