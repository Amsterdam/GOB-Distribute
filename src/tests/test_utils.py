from unittest import mock, TestCase

from gobdistribute.utils import json_loads

class TestUtils(TestCase):
    
    @mock.patch('gobdistribute.utils.json')
    def test_json_loads(self, mock_json):
        json_loads('any json')
        
        mock_json.loads.assert_called_with('any json')

    @mock.patch('gobdistribute.utils.json')
    def test_json_loads_exception(self, mock_json):
        mock_json.loads.side_effect = Exception('Any Exception')
        with self.assertRaises(Exception):
            mock_json.loads.assert_called_with('any json')