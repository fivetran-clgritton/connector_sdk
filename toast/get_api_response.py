import unittest
from unittest.mock import patch, MagicMock
from connector import get_api_response  # Replace 'your_module' with the actual module name


class TestGetApiResponse(unittest.TestCase):

    @patch("requests.get")
    def test_successful_response(self, mock_get):
        """Test successful API response"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {"Toast-Next-Page-Token": "next_token"}
        mock_get.return_value = mock_response

        response, next_token = get_api_response("http://example.com/api", headers={})

        self.assertEqual(response, {"data": "test"})
        self.assertEqual(next_token, "next_token")

    @patch("requests.get")
    def test_401_retry_then_fail(self, mock_get):
        """Test 401 Unauthorized - retry up to max retries, then fail"""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        response, next_token = get_api_response("http://example.com/api", headers={})

        self.assertIsNone(response)
        self.assertIsNone(next_token)
        self.assertEqual(mock_get.call_count, 4)  # Expect 4 calls (1 initial + 3 retries)

    @patch("requests.get")
    def test_403_forbidden(self, mock_get):
        """Test 403 Forbidden - should raise PermissionError"""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        with self.assertRaises(PermissionError):
            get_api_response("http://example.com/api", headers={})

    @patch("requests.get")
    def test_429_too_many_requests(self, mock_get):
        """Test 429 Too Many Requests - should retry after wait time"""
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "2"}

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"data": "test"}
        mock_response_success.headers = {}

        mock_get.side_effect = [mock_response_429, mock_response_success]  # First 429, then success

        with patch("time.sleep", return_value=None):  # Prevent actual sleep
            response, next_token = get_api_response("http://example.com/api", headers={})

        self.assertEqual(response, {"data": "test"})
        self.assertIsNone(next_token)
        self.assertEqual(mock_get.call_count, 2)  # Should retry once

    @patch("requests.get")
    def test_409_conflict(self, mock_get):
        """Test 409 Conflict - should retry without pageToken"""
        mock_response_409 = MagicMock()
        mock_response_409.status_code = 409

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"data": "test"}
        mock_response_success.headers = {}

        mock_get.side_effect = [mock_response_409, mock_response_success]  # First 409, then success

        response, next_token = get_api_response("http://example.com/api", headers={}, params={"pageToken": "abc"})

        self.assertEqual(response, {"data": "test"})
        self.assertIsNone(next_token)
        self.assertEqual(mock_get.call_count, 2)  # Should retry once

    @patch("requests.get")
    def test_400_bad_request(self, mock_get):
        """Test 400 Bad Request - should log and return None"""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "Invalid request"}
        mock_get.return_value = mock_response

        response, next_token = get_api_response("http://example.com/api", headers={})

        self.assertIsNone(response)
        self.assertIsNone(next_token)

if __name__ == "__main__":
    unittest.main()
