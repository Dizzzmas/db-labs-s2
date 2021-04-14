import json

from redis.client import Pipeline

MESSAGE_INDEX = 'message_index'
MESSAGE_HASH_SET = "message"


def create_message(r, message) -> int:
    def create_msg_transaction(p: Pipeline):
        current_id = p.get(MESSAGE_INDEX)
        message_id = int(current_id) + 1
        p.multi()
        p.incr(MESSAGE_INDEX, 1)
        p.hset(MESSAGE_HASH_SET, message_id, json.dumps(message))

    message_id: int = r.transaction(create_msg_transaction, MESSAGE_INDEX)[0]
    return message_id
