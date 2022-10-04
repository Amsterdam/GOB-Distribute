from unittest import mock, TestCase

from gobdistribute.utils import json_loads, get_with_retries


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

    @mock.patch("gobdistribute.utils.EXPORT_API_HOST", "http://exportapihost")
    @mock.patch("gobdistribute.utils.Session")
    @mock.patch("gobdistribute.utils.HTTPAdapter")
    @mock.patch("gobdistribute.utils.Retry")
    def test_get_with_retries(self, mock_retry, mock_httpadapter, mock_session):
        result = get_with_retries("someurl")

        self.assertEqual(result, mock_session.return_value.get.return_value)
        mock_session.return_value.get.assert_called_with('someurl')
        mock_session.return_value.mount.assert_called_with("http://exportapihost", mock_httpadapter.return_value)
        mock_httpadapter.assert_called_with(max_retries=mock_retry.return_value)
        mock_retry.assert_called_with(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
