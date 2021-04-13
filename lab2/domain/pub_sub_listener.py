import threading


EVENT_JOURNAL_CHANNEL = "event_journal"


class Listener(threading.Thread):
    def __init__(self, r):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(["event_journal"])

    def work(self, item):
        print(item['channel'], ":", item['data'])

    def run(self):
        for item in self.pubsub.listen():
            if item['data'] == "KILL":
                self.pubsub.unsubscribe()
                print(self, "unsubscribed and finished")
                break
            else:
                self.work(item)
