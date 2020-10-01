import json

from unittest import TestCase
from unittest.mock import call, mock_open, patch, MagicMock

from gobdistribute.distribute import distribute, _download_sources, _distribute_files, _get_file, _get_config, \
    ObjectDatastore, _get_filenames, _get_export_products


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
        fileset = 'some fileset'

        mock_distribute_config = {
            'any product': 'any config',
            'some fileset': 'another config'
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
            call(conn_info, 'any dir/any product', 'any config', 'any catalogue'),
            call(conn_info, 'any dir/some fileset', 'another config', 'any catalogue'),
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
        distribute(catalogue, fileset)

        self.assertEqual([
            call(conn_info, 'any dir/some fileset', 'another config', 'any catalogue'),
            ],
            mock_download_sources.mock_calls,
            "The method was not called with the correct arguments."
        )
        
        mock_download_sources.reset_mock()

    @patch('gobdistribute.distribute.requests')
    @patch('gobdistribute.distribute.EXPORT_API_HOST', 'http://exportapihost')
    def test_get_export_products(self, mock_requests):
        resp = {'a': 'b', 'c': {'d': 'e'}}
        return_value = MagicMock()
        return_value.text = json.dumps(resp)

        mock_requests.get.return_value = return_value

        self.assertEqual(resp, _get_export_products())
        mock_requests.get.assert_called_with('http://exportapihost/products')
        return_value.raise_for_status.assert_called_once()

    @patch('gobdistribute.distribute._get_export_products')
    def test_get_filenames(self, mock_get_export_products):
        mock_get_export_products.return_value = {
            'catalog1': {
                'collection1': {
                    'product1': [
                        'file1.csv',
                        'file2.shp',
                    ],
                    'product2': [
                        'file3.dat',
                    ],
                },
                'collection2': {
                    'product3': [
                        'file4.dat'
                    ],
                },
                'collection3': {
                    'product4': [
                        'file5.csv',
                        'file6.shp',
                    ],
                    'product5': [
                        'file7.csv',
                    ]
                },
            },
        }

        config = {
            'sources': [
                {
                    'file_name': 'some/filename.csv'
                },
                {
                    'export': {
                        'collection': 'collection1'
                    }
                },
                {
                    'export': {
                        'collection': 'collection3',
                        'products': [
                            'product4'
                        ]
                    }
                }
            ]
        }

        self.assertEqual([
            'some/filename.csv',
            'catalog1/file1.csv',
            'catalog1/file2.shp',
            'catalog1/file3.dat',
            'catalog1/file5.csv',
            'catalog1/file6.shp',
        ], _get_filenames(config, 'catalog1'))

    @patch('gobdistribute.distribute._get_filenames')
    @patch('gobdistribute.distribute._get_file')
    def test_download_sources(self, mock_get_file, mock_get_filenames):
        mock_get_filenames.return_value = ['some/dir/any filename', 'some/other/dir/another filename']
        mock_config = MagicMock()

        mock_get_file.side_effect = [
            ({'name': 'any file'}, 'any file'),
            ({'name': 'another file found with a different name'}, 'another file')
        ]
        
        with patch("builtins.open") as mock_open:
            _download_sources('any connection', 'any directory', mock_config, 'any catalogue')

            self.assertEqual([
                call('any connection', 'some/dir/any filename'),
                call('any connection', 'some/other/dir/another filename'),
                ],
                mock_get_file.mock_calls,
                "The method was not called with the correct arguments."
            )

        mock_open.assert_has_calls([
            call('any directory/any file', 'wb'),
            call('any directory/another file found with a different name', 'wb'),
        ], True)

        mock_get_filenames.assert_called_with(mock_config, 'any catalogue')

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