from unittest import TestCase
from unittest.mock import call, mock_open, patch, MagicMock

from gobdistribute.distribute import distribute, _download_sources, _distribute_files, _get_file, _get_config, \
    ObjectDatastore

@patch('gobdistribute.distribute.logger', MagicMock())
class TestDistribute(TestCase):
    
    @patch('gobdistribute.distribute.get_datastore_config')
    @patch('gobdistribute.distribute.DatastoreFactory.get_datastore')
    @patch('gobdistribute.distribute._get_config')
    @patch('gobdistribute.distribute._download_sources')
    @patch('gobdistribute.distribute._distribute_files')
    @patch('gobdistribute.distribute.CONTAINER_BASE', 'development')
    @patch('gobdistribute.distribute.Path', MagicMock())
    @patch('gobdistribute.distribute.tempfile.gettempdir', lambda: 'any dir')
    def test_distribute(self, mock_distribute_files, mock_download_sources, mock_get_config, mock_get_datastore, mock_get_datastore_config):
        catalogue = 'any catalogue'
        collection = 'any collection'
        product = 'any product'

        mock_distribute_config = {
            'any product': 'any config',
            'another product': 'another config'
        }
        mock_get_config.return_value = mock_distribute_config

        distribute(catalogue)

        mock_get_datastore.assert_called_with(mock_get_datastore_config.return_value)

        connection = mock_get_datastore.return_value.connection

        conn_info = {
            "connection": connection,
            "container": 'development'
        }

        mock_get_config.assert_called_with(conn_info, catalogue)

        self.assertEqual([
            call(conn_info, 'any dir/any product', 'any config', None),
            call(conn_info, 'any dir/another product', 'another config', None),
            ],
            mock_download_sources.mock_calls,
            "The method was not called with the correct arguments."
        )

        self.assertEqual([
            call('any config', mock_download_sources.return_value),
            call('another config', mock_download_sources.return_value),
            ],
            mock_distribute_files.mock_calls,
            "The method was not called with the correct arguments."
        )

        mock_download_sources.reset_mock()
        distribute(catalogue, collection)

        self.assertEqual([
            call(conn_info, 'any dir/any product', 'any config', 'any collection'),
            call(conn_info, 'any dir/another product', 'another config', 'any collection'),
            ],
            mock_download_sources.mock_calls,
            "The method was not called with the correct arguments."
        )
        
        mock_download_sources.reset_mock()
        distribute(catalogue, collection, product)

        self.assertEqual([
            call(conn_info, 'any dir/any product', 'any config', 'any collection'),
            ],
            mock_download_sources.mock_calls,
            "The method was not called with the correct arguments."
        )

    @patch('gobdistribute.distribute._get_file')
    def test_download_sources(self, mock_get_file):
        mock_config = {
            'source': {
                'location': 'any location',
                'collections': {
                    'any collection': {
                        'file_name': 'any filename'
                    },
                    'another collection': {
                        'file_name': 'another filename'
                    }
                }
            }
        }
        
        mock_get_file.side_effect = [('any file info', 'any file'), ('another file info', 'another file')]
        
        with patch("builtins.open", mock_open()) as mock_file:
            _download_sources('any connection', 'any directory', mock_config, collection=None)

            self.assertEqual([
                call('any connection', 'any location/any filename'),
                call('any connection', 'any location/another filename'),
                ],
                mock_get_file.mock_calls,
                "The method was not called with the correct arguments."
            )

        handle = mock_file()
        self.assertEqual([
            call('any file'),
            call('another file'),
            ],
            handle.write.mock_calls,
            "The method was not called with the correct arguments."
        )

        mock_get_file.reset_mock()
        mock_get_file.side_effect = [('any file info', 'any file')]

        with patch("builtins.open", mock_open()) as mock_file:
            _download_sources('any connection', 'any directory', mock_config, collection='another collection')

            self.assertEqual([
                call('any connection', 'any location/another filename'),
                ],
                mock_get_file.mock_calls,
                "The method was not called with the correct arguments."
            )

    @patch('gobdistribute.distribute.get_datastore_config')
    @patch('gobdistribute.distribute.DatastoreFactory.get_datastore')
    @patch('gobdistribute.distribute.CONTAINER_BASE', "containerbase")
    def test_distribute_files(self, mock_get_datastore, mock_get_datastore_config):
        mock_config = {
            'destinations': [
                {       
                    'name': 'any name',
                    'location': 'any location',
                }
            ]
        }
        
        mock_files = ['file1', 'file2']
        
        _distribute_files(mock_config, mock_files)

        mock_get_datastore_config.assert_called_with('any name')
        mock_get_datastore.assert_called_with(mock_get_datastore_config.return_value)

        datastore = mock_get_datastore.return_value

        self.assertEqual([
            call('file1', 'containerbase/any location/file1'),
            call('file2', 'containerbase/any location/file2'),
            ],
            datastore.put_file.mock_calls,
            "The method was not called with the correct arguments."
        )

        # When datastore is of type ObjectDatastore, don't add the base directory
        mock_get_datastore.return_value = MagicMock(spec=ObjectDatastore)

        _distribute_files(mock_config, mock_files)

        self.assertEqual([
            call('file1', 'any location/file1'),
            call('file2', 'any location/file2'),
        ],
            mock_get_datastore.return_value.put_file.mock_calls,
            "The method was not called with the correct arguments."
        )


    @patch('gobdistribute.distribute.get_object')
    @patch('gobdistribute.distribute.get_full_container_list')
    def test_get_file(self, mock_get_full_container_list, mock_get_object):
        conn_info = {
            'connection': "any connection",
            'container': "any container"
        }
        filename = "any filename"

        mock_get_full_container_list.return_value = iter([])
        obj_info, obj = _get_file(conn_info, filename)
        self.assertIsNone(obj_info)
        self.assertIsNone(obj)
        mock_get_object.assert_not_called()

        mock_get_full_container_list.return_value = iter([{'name': filename}])
        mock_get_object.return_value = "get object"
        obj_info, obj = _get_file(conn_info, filename)
        self.assertEqual(obj_info, {'name': filename})
        self.assertEqual(obj, "get object")
        mock_get_object.assert_called_with('any connection', {'name': filename}, 'any container')

        filename = "20201201yz"
        mock_get_full_container_list.return_value = iter([
            {'name': '20201101yz', 'last_modified': '100'},
            {'name': '20201103yz', 'last_modified': '300'},
            {'name': '20201102yz', 'last_modified': '200'},
        ])
        mock_get_object.return_value = "get object"
        obj_info, obj = _get_file(conn_info, filename)
        self.assertEqual(obj_info, {'name': '20201103yz', 'last_modified': '300'})

    @patch('gobdistribute.distribute._get_file')
    def test_get_config(self, mock_get_file):
        conn_info = {
            'connection': "any connection",
            'container': "any container"
        }
        catalogue = "any catalogue"

        mock_get_file.return_value = None, None
        result = _get_config(conn_info, catalogue)
        self.assertEqual(result, {})

        mock_get_file.return_value = None, b"1234"
        result = _get_config(conn_info, catalogue)
        self.assertEqual(result, 1234)

        mock_get_file.return_value = None, b"abc123"
        result = _get_config(conn_info, catalogue)
        self.assertEqual(result, {})