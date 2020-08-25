from gobcore.logging.logger import logger
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, DISTRIBUTE, DISTRIBUTE_QUEUE, DISTRIBUTE_RESULT_KEY
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.notifications import listen_to_notifications, get_notification
from gobcore.workflow.start_workflow import start_workflow

from gobdistribute.distribute import distribute


def handle_distribute_msg(msg):
    header = msg['header']
    logger.configure(msg, "DISTRIBUTE")

    distribute(catalogue=header['catalogue'], collection=header.get('collection'))

    return {
        "header": msg.get("header"),
        "summary": {
            "warnings": logger.get_warnings(),
            "errors": logger.get_errors()
        },
        "contents": None
    }


def distribute_on_export_test(msg):
    """
    On a successfull export test, distribute the files

    :param msg:
    :return:
    """
    notification = get_notification(msg)

    # Start an export cat-col to db workflow to update the analysis database
    workflow = {
        'workflow_name': DISTRIBUTE
    }
    arguments = {
        'catalogue': notification.contents.get('catalogue'),
        'collection': notification.contents.get('collection'),
        'product': notification.contents.get('product'),
        'process_id': notification.header.get('process_id'),
    }
    start_workflow(workflow, arguments)


SERVICEDEFINITION = {
    'distribute_request': {
        'queue': DISTRIBUTE_QUEUE,
        'handler': handle_distribute_msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': DISTRIBUTE_RESULT_KEY,
        }
    },
    'distribute': {
        'queue': lambda: listen_to_notifications("distribute", 'export_test'),
        'handler': distribute_on_export_test
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Distribute")


init()
