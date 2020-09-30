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
    def test_handle_distribute_msg(self, mock_distribute):

        msg = {
            "header": {
                "catalogue": "catalogue",
                "fileset": "fileset"
            },
            "any other arg": "any other arg",
        }

        __main__.handle_distribute_msg(msg)
        
        mock_distribute.assert_called_with(
            catalogue="catalogue",
            fileset="fileset")

    @mock.patch("gobdistribute.__main__.get_notification")
    @mock.patch("gobdistribute.__main__.start_workflow")
    def test_distribute_on_export_test(self, mock_start_workflow, mock_get_notification):
        msg = mock.MagicMock()

        mock_get_notification.return_value.contents = {
            'catalogue': 'CAT',
        }
        mock_get_notification.return_value.header = {
            'process_id': 'PROCESS_ID'
        }

        __main__.distribute_on_export_test(msg)
        mock_get_notification.assert_called_with(msg)

        mock_start_workflow.assert_called_with(
            {
                'workflow_name': __main__.DISTRIBUTE
            },
            {
                'catalogue': 'CAT',
                'process_id': 'PROCESS_ID'
            }
        )
