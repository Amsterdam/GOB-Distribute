from unittest import mock, TestCase

from gobdistribute import __main__


@mock.patch('gobdistribute.__main__.logger', mock.MagicMock())
class TestMain(TestCase):

    @mock.patch("gobdistribute.__main__.messagedriven_service")
    def test_messagedriven_service(self, mocked_messagedriven_service):
        from gobdistribute import __main__ as module

        with mock.patch.object(module, '__name__', '__main__'):
            __main__.init()
            mocked_messagedriven_service.assert_called_with(__main__.SERVICEDEFINITION, "Distribute")

    @mock.patch('gobdistribute.__main__.distribute')
    def test_main(self, mock_distribute):

        msg = {
            "header": {
                "catalogue": "catalogue",
                "collection": "collection"
            },
            "any other arg": "any other arg",
        }

        __main__.handle_distribute_msg(msg)
        
        mock_distribute.assert_called_with(
            catalogue="catalogue",
            collection="collection")