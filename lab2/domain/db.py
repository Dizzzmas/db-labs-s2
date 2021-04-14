from functools import wraps
from flask_smorest import abort
from flask import request
from redis import Redis

from domain.pub_sub_listeners import EventJournalListener, MessageQueueListener
from domain.user import REGULAR_USERS_SET, ADMIN_USERS_SET

REGULAR_USERS = ["Alice", "Malory"]
ADMIN_USERS = ["Dizzzmas", "Ilya"]


def seed_db(r: Redis):
    r.sadd(REGULAR_USERS_SET, *REGULAR_USERS)
    r.sadd(ADMIN_USERS_SET, *ADMIN_USERS)


def start_listeners(r: Redis):
    pubsub_listener = EventJournalListener(r)
    message_queue_listener = MessageQueueListener(r)
    pubsub_listener.start()
    message_queue_listener.start()
