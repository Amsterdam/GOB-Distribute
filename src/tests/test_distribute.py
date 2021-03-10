import json

from unittest import TestCase
from unittest.mock import call, patch, MagicMock

from gobdistribute.distribute import distribute, _download_sources, _distribute_files, _get_file, _get_config, \
    ObjectDatastore, _get_filenames, _get_export_products, GOB_OBJECTSTORE, _get_datastore, \
    _apply_filename_replacements, _expand_filename_wildcard, _distribute_file


@patch('gobdistribute.distribute.logger', MagicMock())
class TestDistribute(TestCase):

    @patch('gobdistribute.distribute._get_datastore')
    @patch('gobdistribute.distribute._get_config')
    @patch('gobdistribute.distribute._get_filenames')
    @patch('gobdistribute.distribute._download_sources')
    @patch('gobdistribute.distribute._distribute_files')
    @patch('gobdistribute.distribute.CONTAINER_BASE', 'THE_CONTAINER')
    @patch('gobdistribute.distribute.tempfile.gettempdir', lambda: '/tmpdir')
    def test_distribute(self, mock_distribute_files, mock_download_sources, mock_get_filenames,
                        mock_get_config, mock_get_datastore):
        catalogue = 'any catalogue'
        fileset = 'fileset_a'

        mock_get_datastore.return_value = (MagicMock(), 'BASE_DIR/')
        mock_download_sources.return_value = [
            ('dst_location/source1.csv', 'path/to/source1.csv'),
            ('dst_location/source2.csv', 'path/to/source2.csv'),
        ]

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
            ('BASE_DIR/location/a/dst_location/source1.csv', 'path/to/source1.csv'),
            ('BASE_DIR/location/a/dst_location/source2.csv', 'path/to/source2.csv'),
        ]
        mapping_b = [
            ('BASE_DIR/location/b/dst_location/source1.csv', 'path/to/source1.csv'),
            ('BASE_DIR/location/b/dst_location/source2.csv', 'path/to/source2.csv'),
        ]
        mapping_c = [
            ('BASE_DIR/location/c/dst_location/source1.csv', 'path/to/source1.csv'),
            ('BASE_DIR/location/c/dst_location/source2.csv', 'path/to/source2.csv'),
        ]

        mock_get_datastore.assert_has_calls([
            call(GOB_OBJECTSTORE),
            call('destA'),
            call('destB'),
            call('destC'),
        ])

        conn_info = {
            "connection": mock_get_datastore.return_value[0].connection,
            "container": 'THE_CONTAINER'
        }

        mock_get_config.assert_called_with(conn_info, catalogue, 'THE_CONTAINER')
        mock_get_filenames.assert_has_calls([
            call(conn_info, mock_get_config.return_value['fileset_a'], catalogue),
            call(conn_info, mock_get_config.return_value['fileset_b'], catalogue),
        ])
        mock_download_sources.assert_has_calls([
            call(conn_info, '/tmpdir/fileset_a', mock_get_filenames()),
            call(conn_info, '/tmpdir/fileset_b', mock_get_filenames()),
        ])

        # Reset mocks. Test with only one fileset
        mock_download_sources.reset_mock()
        mock_get_filenames.reset_mock()

        distribute(catalogue, fileset)

        mock_get_filenames.assert_has_calls([
            call(conn_info, mock_get_config.return_value['fileset_a'], catalogue),
        ])

        mock_download_sources.assert_has_calls([
            call(conn_info, '/tmpdir/fileset_a', mock_get_filenames()),
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

    @patch('gobdistribute.distribute.get_full_container_list')
    def test_expand_filename_wildcard(self, mock_get_list):
        conn_info = {'connection': 'CONNECTION', 'container': 'CONTAINER'}
        mock_get_list.return_value = [
            {'name': 'dir/a.csv', 'content_type': ''},
            {'name': 'dir/b.csv', 'content_type': ''},
            {'name': 'dir/a.shp', 'content_type': ''},
            {'name': 'dir', 'content_type': 'application/directory'},  # should be ignored
            {'name': 'dir/b.shp', 'content_type': ''},
            {'name': 'anotherdir/a.csv', 'content_type': ''},
            {'name': 'anotherdir/b.shp', 'content_type': ''},
        ]

        self.assertEqual([
            'dir/a.csv',
            'dir/b.csv',
        ], _expand_filename_wildcard(conn_info, 'dir/*.csv'))

        self.assertEqual([
            'dir/a.csv',
            'dir/a.shp',
        ], _expand_filename_wildcard(conn_info, 'dir/a.*'))

        self.assertEqual([
            'dir/a.csv',
            'dir/b.csv',
            'dir/a.shp',
            'dir/b.shp',
        ], _expand_filename_wildcard(conn_info, 'dir/*'))

        self.assertEqual([
            'dir/a.csv',
            'dir/b.csv',
            'dir/a.shp',
            'dir/b.shp',
            'anotherdir/a.csv',
            'anotherdir/b.shp',
        ], _expand_filename_wildcard(conn_info, '*'))

        mock_get_list.assert_called_with('CONNECTION', 'CONTAINER')

    @patch('gobdistribute.distribute._expand_filename_wildcard')
    @patch('gobdistribute.distribute._get_export_products')
    def test_get_filenames(self, mock_get_export_products, mock_expand_wildcard):
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
        mock_expand_wildcard.return_value = ['some/dir/a.csv', 'some/dir/b.csv']

        config = {
            'sources': [
                {
                    'file_name': 'some/filename.csv',
                    'base_dir': 'base_dir/',
                },
                {
                    'file_name': '*.csv',
                    'base_dir': 'some/dir',
                },
                {
                    'file_name': 'other/filename_no_basedir.csv',
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
        conn_info = {'conn': 'info'}

        self.assertEqual([
            ('some/filename.csv', 'base_dir/some/filename.csv'),
            ('a.csv', 'some/dir/a.csv'),
            ('b.csv', 'some/dir/b.csv'),
            ('other/filename_no_basedir.csv', 'other/filename_no_basedir.csv'),
            ('catalog1/file1.csv', 'catalog1/file1.csv'),
            ('catalog1/file2.shp', 'catalog1/file2.shp'),
            ('catalog1/file3.dat', 'catalog1/file3.dat'),
            ('catalog1/file5.csv', 'catalog1/file5.csv'),
            ('catalog1/file6.shp', 'catalog1/file6.shp')
        ], _get_filenames(conn_info, config, 'catalog1'))
        mock_get_export_products.assert_called_with('catalog1')
        mock_expand_wildcard.assert_called_with(conn_info, 'some/dir/*.csv')

    @patch('gobdistribute.distribute.Path')
    @patch('gobdistribute.distribute._get_file')
    def test_download_sources(self, mock_get_file, mock_path):
        filenames = [
            ('some/dir/any filename', 'src/file/name1.csv'),
            ('some/other/dir/another filename', 'src/file/name2.csv')
        ]

        mock_get_file.side_effect = [
            ({'name': 'any file'}, 'any file'),
            ({'name': 'another file found with a different name'}, 'another file')
        ]

        with patch("builtins.open") as mock_open:
            res = _download_sources('any connection', 'any directory', filenames)

            self.assertEqual([
                call('any connection', 'src/file/name1.csv'),
                call('any connection', 'src/file/name2.csv'),
            ],
                mock_get_file.mock_calls,
                "The method was not called with the correct arguments."
            )

        self.assertEqual([
            ('some/dir/any filename', 'any directory/some/dir/any filename'),
            ('some/other/dir/another filename', 'any directory/some/other/dir/another filename'),
        ], res)

        mock_open.assert_has_calls([
            call('any directory/some/dir/any filename', 'wb'),
            call('any directory/some/other/dir/another filename', 'wb'),
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

    @patch('gobdistribute.distribute._distribute_file')
    def test_distribute_files(self, mock_distribute_file):
        datastore = MagicMock(spec=ObjectDatastore)
        datastore.list_files.return_value = [
            "some/dir/a/b/file12345678.txt",
            "some/dir/a/b/file90123453.txt",
            "some/other/dir/with/file.txt",
        ]
        mapping = [
            ('a/b/dstfile.txt', 'somelocalfile.txt'),
            ('a/b/file11112233.txt', 'someotherlocalfile.txt'),
        ]

        _distribute_files(datastore, mapping, 'some/dir')

        mock_distribute_file.assert_has_calls([
            call(datastore, 'somelocalfile.txt', 'some/dir/a/b/dstfile.txt', []),
            call(datastore, 'someotherlocalfile.txt', 'some/dir/a/b/file11112233.txt', [
                'some/dir/a/b/file12345678.txt',
                'some/dir/a/b/file90123453.txt',
            ])
        ])

    def test_distribute_file(self):
        datastore = MagicMock(spec=ObjectDatastore)
        local_file = 'localfile.txt'
        destination_filename = 'destination_file.txt'
        existing_files = [
            'existingfile1.txt',
            'existingfile2.txt',
        ]

        _distribute_file(datastore, local_file, destination_filename, existing_files)
        datastore.delete_file.assert_has_calls([
            call('existingfile1.txt'),
            call('existingfile2.txt'),
        ])
        datastore.put_file.assert_called_with('localfile.txt', 'destination_file.txt')

        datastore.delete_file.reset_mock()
        datastore.put_file.reset_mock()
        datastore.delete_file.side_effect = OSError

        _distribute_file(datastore, local_file, destination_filename, existing_files)
        datastore.delete_file.assert_has_calls([
            call('existingfile1.txt'),
        ])
        datastore.put_file.assert_not_called()

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
        environment = "any container"

        mock_get_file.return_value = None, None
        result = _get_config(conn_info, catalogue, environment)
        self.assertEqual(result, {})

        mock_get_file.assert_called_with(conn_info, "distribute.any container.any catalogue.json")

        mock_get_file.return_value = None, b"1234"
        result = _get_config(conn_info, catalogue, environment)
        self.assertEqual(result, 1234)

        mock_get_file.return_value = None, b"abc123"
        result = _get_config(conn_info, catalogue, environment)
        self.assertEqual(result, {})
