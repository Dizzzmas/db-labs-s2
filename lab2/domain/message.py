import json
from enum import Enum, unique
from typing import TypedDict

from redis.client import Pipeline

from domain.pub_sub_listeners import MESSAGE_QUEUE_CHANNEL

MESSAGE_INDEX = "message_index"  # Stores the id of the latest sent message. Used to generate new ids.
MESSAGE_QUEUE = (
    "message_queue"  # List with message ids for in-order spam-checks and delivery
)
INBOUND_MESSAGES_LIST = "inbound_messages"  # Pairs username->[received_message_ids]
OUTBOUND_MESSAGES_LIST = "outbound_messages"  # Pairs username->[send_message_ids]
MESSAGE_HASH = "message"  # Pairs message_id->message_object
ENQUEUED_MESSAGES_SET = "messages:enqueued"


@unique
class MessageDeliveryStatus(Enum):
    queued = "queued"
    checking_for_spam = "checking_for_spam"
    blocked_for_spam = "blocked_for_spam"
    sent = "sent"
    delivered = "delivered"


class RawMessage(TypedDict):
    """Message without the id.
    Used when we still haven't inserted the message into Redis.
    """

    sender: str
    recipient: str
    content: str


class Message(RawMessage):
    """
    Message with the id.
    Used when we fetch the message from Redis.
    """

    id: int


def create_message(r, message: RawMessage) -> int:
    """ Create a new message in Redis Hash and enqueue it for spam detection an eventual delivery. """

    def create_msg_transaction(p: Pipeline):
        current_id = p.get(MESSAGE_INDEX)
        new_id = int(current_id) + 1
        p.multi()
        # Increment the message_id and assign the newly created message to it
        p.incr(MESSAGE_INDEX, 1)
        p.hset(MESSAGE_HASH, new_id, json.dumps(message))
        # Mark the message as enqueued
        p.sadd(ENQUEUED_MESSAGES_SET, new_id)
        # Push to the queue for processing
        p.lpush(MESSAGE_QUEUE, new_id)
        # Notify the listener that it should call a worker to process a new message in the queue
        p.publish(MESSAGE_QUEUE_CHANNEL, new_id)

    # More on Redis transactions here: https://github.com/andymccurdy/redis-py/#pipelines
    message_id: int = r.transaction(create_msg_transaction, MESSAGE_INDEX)[0]
    return message_id
