import json
import random
import threading
from abc import abstractmethod

from redis import Redis

from domain.message import MESSAGE_QUEUE, RawMessage, process_enqueued_message
from domain.redis_structures import (
    EVENT_JOURNAL_CHANNEL,
    MESSAGE_QUEUE_CHANNEL,
    MESSAGE_HASH,
    ENQUEUED_MESSAGES_SET,
    SPAM_MESSAGES_SET,
    BEING_SPAM_CHECKED_MESSAGES_SET,
    DELIVERED_MESSAGES_SET,
    USERS_BY_DELIVERED_MESSAGES_SORTED_SET,
    USERS_BY_SPAM_MESSAGES_SORTED_SET,
    EVENT_JOURNAL_LIST,
)


class PubSubListener(threading.Thread):
    def __init__(self, r: Redis):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()

    @abstractmethod
    def work(self, item):
        ...

    def run(self):
        for item in self.pubsub.listen():
            if item["data"] == "KILL":
                self.pubsub.unsubscribe()
                print(self, "unsubscribed and finished")
                break
            else:
                self.work(item)


class EventJournalListener(PubSubListener):
    """ Record such events: user logins/logouts, message spam checks. """

    def __init__(self, r: Redis):
        super().__init__(r)
        self.pubsub.subscribe([EVENT_JOURNAL_CHANNEL])

    def work(self, item):
        if item["type"] != "message":
            return
        message = item["data"]
        print(item["channel"], ":", item["data"])
        self.redis.lpush(EVENT_JOURNAL_LIST, message)


class MessageQueueListener(PubSubListener):
    """
    When a message gets enqueued, fetch it with LPOP and check for spam.
    Then send it to the inbound/outbound user inboxes.
    """

    def __init__(self, r: Redis):
        super().__init__(r)
        self.pubsub.subscribe([MESSAGE_QUEUE_CHANNEL])

    def work(self, item):
        if item["type"] != "message":
            return
        process_enqueued_message(self.redis)
