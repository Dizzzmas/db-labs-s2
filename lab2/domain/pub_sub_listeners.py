import threading
from abc import abstractmethod

from redis import Redis

EVENT_JOURNAL_CHANNEL = "event_journal"
MESSAGE_QUEUE_CHANNEL = "message_queue"


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
        print(item["channel"], ":", item["data"])


class MessageQueueListener(PubSubListener):
    """
    When a message gets enqueued, fetch it with LPOP and check for spam.
    Then send it to the inbound/outbound user inboxes.
    """

    def __init__(self, r: Redis):
        super().__init__(r)
        self.pubsub.subscribe([MESSAGE_QUEUE_CHANNEL])

    def work(self, item):
        print(item["channel"], ":", item["data"])
        f = "s"
