import unittest
from unittest.mock import patch, MagicMock
import uuid

# Import your function and dependencies here
from connector import process_child, flatten_fields, stringify_lists, op

def flatten_fields(fields, row):
    """Mocked flatten_fields function for testing."""
    return row

def stringify_lists(row):
    """Mocked stringify_lists function for testing."""
    return row

class MockOp:
    """Mocked database operation class."""
    @staticmethod
    def upsert(table, data):
        return {"table": table, "data": data}

op = MockOp()

class TestProcessChild(unittest.TestCase):

    @patch("connector.flatten_fields", side_effect=flatten_fields)
    @patch("connector.stringify_lists", side_effect=stringify_lists)
    @patch("connector.op.upsert", side_effect=op.upsert)
    def test_process_child_basic(self, mock_upsert, mock_stringify, mock_flatten):
        parent_data = [{"guid": "123", "customer": {"name": "John"}}]
        table_name = "orders_check"
        id_field_name = "parent_id"
        id_field = "root_1"

        results = list(process_child(parent_data, table_name, id_field_name, id_field))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["table"], "orders_check")
        self.assertEqual(results[0]["data"]["guid"], "123")
        self.assertEqual(results[0]["data"]["parent_id"], "root_1")

    @patch("connector.flatten_fields", side_effect=flatten_fields)
    @patch("connector.stringify_lists", side_effect=stringify_lists)
    @patch("connector.op.upsert", side_effect=op.upsert)
    def test_process_child_with_nested(self, mock_upsert, mock_stringify, mock_flatten):
        parent_data = [{
            "guid": "456",
            "selections": [{"guid": "789", "item": "Laptop"}],
            "customer": {"name": "Alice"}
        }]
        table_name = "orders_check"
        id_field_name = "parent_id"
        id_field = "root_2"

        results = list(process_child(parent_data, table_name, id_field_name, id_field))

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["table"], "orders_check_selection")
        # self.assertEqual(results[0]["data"]["parent_id"], "456")
        self.assertEqual(results[0]["data"]["guid"], "789")
        self.assertEqual(results[1]["table"], "orders_check")
        self.assertEqual(results[1]["data"]["guid"], "456")

    @patch("connector.flatten_fields", side_effect=flatten_fields)
    @patch("connector.stringify_lists", side_effect=stringify_lists)
    @patch("connector.op.upsert", side_effect=op.upsert)
    def test_process_child_null_guid_generation(self, mock_upsert, mock_stringify, mock_flatten):
        parent_data = [{"guid": None}]
        table_name = "orders_check_selection_applied_tax"
        id_field_name = "parent_id"
        id_field = "root_3"

        results = list(process_child(parent_data, table_name, id_field_name, id_field))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["table"], "orders_check_selection_applied_tax")
        self.assertTrue(results[0]["data"]["guid"].startswith("gen-"))

    @patch("connector.flatten_fields", side_effect=flatten_fields)
    @patch("connector.stringify_lists", side_effect=stringify_lists)
    @patch("connector.op.upsert", side_effect=op.upsert)
    def test_process_child_handles_missing_keys(self, mock_upsert, mock_stringify, mock_flatten):
        parent_data = [{"guid": "001"}]  # No selections, should not recurse
        table_name = "orders_check"
        id_field_name = "parent_id"
        id_field = "root_4"

        results = list(process_child(parent_data, table_name, id_field_name, id_field))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["table"], "orders_check")
        self.assertEqual(results[0]["data"]["guid"], "001")

if __name__ == '__main__':
    unittest.main()
