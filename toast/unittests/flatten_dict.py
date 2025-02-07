import unittest

def flatten_dict(parent_row: dict, dict_field: dict, prefix: str):
    """
    Flattens a nested dictionary into a single-level dictionary.

    :param parent_row: The dictionary to update with flattened fields.
    :param dict_field: The dictionary to flatten.
    :param prefix: The prefix to add to new keys in parent_row.
    :return: The updated parent_row with dict_field flattened.
    """
    # Fields that should not have a prefix applied
    fields_to_not_prefix = ["refundDetails"]

    if not dict_field:  # Quick exit for empty dictionaries
        return parent_row

    for key, value in dict_field.items():
        # Avoid redundant prefixing (e.g., "refund_refundAmount")
        if key.startswith(prefix):
            new_key = key  # Keep it unchanged
        elif prefix in fields_to_not_prefix:
            new_key = key  # Keep it unchanged for exempted fields
        else:
            new_key = f"{prefix}_{key}"  # Apply prefix normally

        if isinstance(value, dict):  # If value is another dictionary, recurse
            flatten_dict(parent_row, value, new_key)
        else:
            parent_row[new_key] = value  # Store non-dict values

    return parent_row

class TestFlattenDict(unittest.TestCase):

    def test_flatten_dict_simple(self):
        """Test flattening a dictionary with single-level keys."""
        parent = {}
        nested = {"key1": "value1", "key2": "value2"}
        expected = {"prefix_key1": "value1", "prefix_key2": "value2"}
        self.assertEqual(flatten_dict(parent, nested, "prefix"), expected)

    def test_flatten_dict_nested(self):
        """Test flattening a nested dictionary (multi-level)."""
        parent = {}
        nested = {"key1": {"subkey1": "value1"}, "key2": "value2"}
        expected = {"prefix_key1_subkey1": "value1", "prefix_key2": "value2"}
        self.assertEqual(flatten_dict(parent, nested, "prefix"), expected)

    def test_flatten_dict_deeply_nested(self):
        """Test flattening a deeply nested dictionary."""
        parent = {}
        nested = {"key1": {"subkey1": {"subsubkey1": "value1"}}, "key2": "value2"}
        expected = {"prefix_key1_subkey1_subsubkey1": "value1", "prefix_key2": "value2"}
        self.assertEqual(flatten_dict(parent, nested, "prefix"), expected)

    def test_flatten_dict_empty(self):
        """Test flattening an empty dictionary (should return unchanged parent)."""
        parent = {}
        nested = {}
        expected = {}
        self.assertEqual(flatten_dict(parent, nested, "prefix"), expected)

    def test_flatten_dict_no_dict_values(self):
        """Test flattening a dictionary with non-dict values (numbers, booleans)."""
        parent = {}
        nested = {"key1": 123, "key2": True}
        expected = {"prefix_key1": 123, "prefix_key2": True}
        self.assertEqual(flatten_dict(parent, nested, "prefix"), expected)

    def test_flatten_dict_no_prefix_field(self):
        """Test that fields in `fields_to_not_prefix` do not get prefixed."""
        parent = {}
        nested = {"amount": 100, "reason": "Refund issued"}
        expected = {"amount": 100, "reason": "Refund issued"}  # No prefix
        self.assertEqual(flatten_dict(parent, nested, "refundDetails"), expected)

    def test_flatten_dict_no_prefix_field_with_nested(self):
        """Test `fields_to_not_prefix` handling nested dictionaries correctly."""
        parent = {}
        nested = {"amount": 100, "details": {"code": "R123", "source": "Customer"}}
        expected = {"amount": 100, "details_code": "R123", "details_source": "Customer"}
        self.assertEqual(flatten_dict(parent, nested, "refundDetails"), expected)

    def test_flatten_dict_avoid_redundant_prefix(self):
        """Test that keys already containing the prefix are not duplicated."""
        parent = {}
        nested = {
            "refundAmount": 100,  # Should not become "refund_refundAmount"
            "refundDetails": {"code": "R123", "source": "Customer"},
            "nestedKey": {"subKey": "value1"}
        }
        expected = {
            "refundAmount": 100,  # No redundant prefix
            "code": "R123",
            "source": "Customer",
            "refund_nestedKey_subKey": "value1"
        }
        self.assertEqual(flatten_dict(parent, nested, "refund"), expected)

if __name__ == '__main__':
    unittest.main()
