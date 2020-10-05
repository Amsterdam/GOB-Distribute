import json

from unittest import TestCase
from unittest.mock import call, mock_open, patch, MagicMock

from gobdistribute.distribute import distribute, _download_sources, _distribute_files, _get_file, _get_config, \
    ObjectDatastore, _get_filenames, _get_export_products, GOB_OBJECTSTORE, _get_datastore, \
    _apply_filename_replacements, _delete_old_files


@patch('gobdistribute.distribute.logger', MagicMock())
class TestDistribute(TestCase):

    @patch('gobdistribute.distribute._get_datastore')
    @patch('gobdistribute.distribute._get_config')
    @patch('gobdistribute.distribute._delete_old_files')
    @patch('gobdistribute.distribute._get_filenames')
    @patch('gobdistribute.distribute._download_sources')
    @patch('gobdistribute.distribute._distribute_files')
    @patch('gobdistribute.distribute.CONTAINER_BASE', 'development')
    @patch('gobdistribute.distribute.tempfile.gettempdir', lambda: '/tmpdir')
    def test_distribute(self, mock_distribute_files, mock_download_sources, mock_get_filenames, mock_delete_old_files,
                        mock_get_config, mock_get_datastore):
        catalogue = 'any catalogue'
        fileset = 'fileset_a'

        mock_get_datastore.return_value = (MagicMock(), 'BASE_DIR/')
        mock_download_sources.return_value = ['path/to/source1.csv', 'path/to/source2.csv']

        mock_get_config.return_value = {
            'fileset_a': {
                'sources': [],
                'destinations': [
                    {'name': 'destA', 'location': 'location/a'},
                    {'name': 'destB', 'location': 'location/b'},
                ]
            },
            'fileset_b': {
                'sources': [],
                'destinations': [
                    {'name': 'destC', 'location': 'location/c'}
                ]
            }
        }

        distribute(catalogue)

        mapping_a = [
            ('path/to/source1.csv', 'BASE_DIR/location/a/source1.csv'),
            ('path/to/source2.csv', 'BASE_DIR/location/a/source2.csv'),
        ]
        mapping_b = [
            ('path/to/source1.csv', 'BASE_DIR/location/b/source1.csv'),
            ('path/to/source2.csv', 'BASE_DIR/location/b/source2.csv'),
        ]
        mapping_c = [
            ('path/to/source1.csv', 'BASE_DIR/location/c/source1.csv'),
            ('path/to/source2.csv', 'BASE_DIR/location/c/source2.csv'),
        ]

        mock_get_datastore.assert_has_calls([
            call(GOB_OBJECTSTORE),
            call('destA'),
            call('destB'),
            call('destC'),
        ])

        conn_info = {
            "connection": mock_get_datastore.return_value[0].connection,
            "container": 'development'
        }

        mock_get_config.assert_called_with(conn_info, catalogue)
        mock_get_filenames.assert_has_calls([
            call(mock_get_config.return_value['fileset_a'], catalogue),
            call(mock_get_config.return_value['fileset_b'], catalogue),
        ])
        mock_download_sources.assert_has_calls([
            call(conn_info, '/tmpdir/fileset_a', mock_get_filenames()),
            call(conn_info, '/tmpdir/fileset_b', mock_get_filenames()),
        ])
        mock_delete_old_files.assert_has_calls([
            call(mock_get_datastore.return_value[0], 'location/a', mapping_a),
            call(mock_get_datastore.return_value[0], 'location/b', mapping_b),
            call(mock_get_datastore.return_value[0], 'location/c', mapping_c),
        ])

        # Reset mocks. Test with only one fileset
        mock_download_sources.reset_mock()
        mock_get_filenames.reset_mock()
        mock_delete_old_files.reset_mock()

        distribute(catalogue, fileset)

        mock_get_filenames.assert_has_calls([
            call(mock_get_config.return_value['fileset_a'], catalogue),
        ])

        mock_download_sources.assert_has_calls([
            call(conn_info, '/tmpdir/fileset_a', mock_get_filenames()),
        ])

        mock_delete_old_files.assert_has_calls([
            call(mock_get_datastore.return_value[0], 'location/a', mapping_a),
            call(mock_get_datastore.return_value[0], 'location/b', mapping_b),
        ])

    @patch('gobdistribute.distribute.requests')
    @patch('gobdistribute.distribute.EXPORT_API_HOST', 'http://exportapihost')
    def test_get_export_products(self, mock_requests):
        resp = {'a': 'b', 'c': {'d': 'e'}}
        return_value = MagicMock()
        return_value.text = json.dumps(resp)

        mock_requests.get.return_value = return_value

        self.assertEqual(resp['c'], _get_export_products('c'))
        mock_requests.get.assert_called_with('http://exportapihost/products')
        return_value.raise_for_status.assert_called_once()

    @patch('gobdistribute.distribute._get_export_products')
    def test_get_filenames(self, mock_get_export_products):
        mock_get_export_products.return_value = {
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
        mock_get_export_products.assert_called_with('catalog1')

    @patch('gobdistribute.distribute.Path')
    @patch('gobdistribute.distribute._get_file')
    def test_download_sources(self, mock_get_file, mock_path):
        filenames = ['some/dir/any filename', 'some/other/dir/another filename']

        mock_get_file.side_effect = [
            ({'name': 'any file'}, 'any file'),
            ({'name': 'another file found with a different name'}, 'another file')
        ]
        
        with patch("builtins.open") as mock_open:
            _download_sources('any connection', 'any directory', filenames)

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

    @patch('gobdistribute.distribute.get_datastore_config')
    @patch('gobdistribute.distribute.DatastoreFactory.get_datastore')
    @patch('gobdistribute.distribute.CONTAINER_BASE', "containerbase")
    def test_get_datastore(self, mock_get_datastore, mock_get_datastore_config):

        res = _get_datastore('any name')
        mock_get_datastore_config.assert_called_with('any name')
        mock_get_datastore.assert_called_with(mock_get_datastore_config.return_value)
        mock_get_datastore().connect.assert_called_once()

        self.assertEqual((mock_get_datastore(), "containerbase/"), res)

        # Type ObjectDatastore, no base dir
        mock_get_datastore.return_value = MagicMock(spec=ObjectDatastore)
        self.assertEqual((mock_get_datastore(), ""), _get_datastore('any name'))

    def test_apply_filename_replacements(self):
        testcases = [
            ('aa12345678bb', 'aa{DATE}bb'),
            ('aa1234567bb', 'aa1234567bb'),
        ]

        for inp, outp in testcases:
            self.assertEqual(outp, _apply_filename_replacements(inp))

    @patch('gobdistribute.distribute._apply_filename_replacements', lambda x: f"_{x}")
    def test_delete_old_files(self):
        datastore = MagicMock(spec=ObjectDatastore)
        datastore.can_list_file.return_value = False
        datastore.can_delete_file.return_value = False

        mapping = [
            ('src a', 'dst 1'),
            ('src b', 'dst 2'),
        ]

        _delete_old_files(datastore, 'some location', mapping)

        datastore.list_files.assert_not_called()

        datastore.can_list_file.return_value = True
        datastore.can_delete_file.return_value = True
        datastore.list_files.return_value = ['dst 3', 'dst 2']

        _delete_old_files(datastore, 'some location', mapping)
        datastore.delete_file.assert_has_calls([call('dst 2')])
        datastore.delete_file.assert_called_once()

    def test_distribute_files(self):
        datastore = MagicMock(spec=ObjectDatastore)
        mapping = [
            ('src a', 'dst 1'),
            ('src b', 'dst 2'),
        ]

        _distribute_files(datastore, mapping)

        datastore.put_file.assert_has_calls([
            call('src a', 'dst 1'),
            call('src b', 'dst 2'),
        ])

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