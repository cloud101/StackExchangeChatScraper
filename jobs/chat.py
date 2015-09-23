#!/usr/bin/env python
import logging
import logging.handlers
import os
import sys
import chatexchange.client
import chatexchange.events
from database.Elastic import ElasticManager
import time
logger = logging.getLogger(__name__)


def main():


    # Run `. setp.sh` to set the below testing environment variables

    host_id = 'stackexchange.com'
    room_id = '151'  # Charcoal Chatbot Sandbox

    if 'ChatExchangeU' in os.environ:
        email = os.environ['ChatExchangeU']
    else:
        email = raw_input("Email: ")
    if 'ChatExchangeP' in os.environ:
        password = os.environ['ChatExchangeP']
    else:
        password = raw_input("Password: ")

    client = chatexchange.client.Client(host_id)
    client.login(email, password)

    room = client.get_room(room_id)
    room.join()
    room.watch(on_message)
    while True:
        time.sleep(1)
    client.logout()


def on_message(message, client):
    if not isinstance(message, chatexchange.events.MessagePosted):
        # Ignore non-message_posted events.
        logger.debug("event: %r", message)
        return
    logger.info(ElasticManager.index_message(message.__dict__["message"].jsonify()))


if __name__ == '__main__':
    main(*sys.argv[1:])
