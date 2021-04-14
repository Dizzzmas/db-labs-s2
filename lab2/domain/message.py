import json
from enum import Enum, unique
from typing import TypedDict

from redis.client import Pipeline, Redis
import random
from time import sleep

from domain.redis_structures import (
    MESSAGE_QUEUE_CHANNEL,
    MESSAGE_QUEUE,
    ENQUEUED_MESSAGES_SET,
    MESSAGE_HASH,
    MESSAGE_INDEX,
    OUTBOUND_MESSAGES_LIST,
    BEING_SPAM_CHECKED_MESSAGES_SET,
    SPAM_MESSAGES_SET,
    EVENT_JOURNAL_CHANNEL,
    USERS_BY_SPAM_MESSAGES_SORTED_SET,
    DELIVERED_MESSAGES_SET,
    USERS_BY_DELIVERED_MESSAGES_SORTED_SET,
    INBOUND_MESSAGES_LIST,
)


random.seed(422)


@unique
class MessageDeliveryStatus(Enum):
    queued = "queued"
    checking_for_spam = "checking_for_spam"
    blocked_for_spam = "blocked_for_spam"
    sent = "sent"
    delivered = "delivered"


class RawMessage(TypedDict):
    """Message without the id."""

    sender: str
    recipient: str
    content: str


class Message(RawMessage):
    """
    Message with the id.
    """

    id: int


def create_message(r, message: RawMessage) -> int:
    """ Create a new message in Redis Hash and enqueue it for spam detection an eventual delivery. """

    def create_msg_transaction(p: Pipeline):
        current_id = p.get(MESSAGE_INDEX)
        new_id = int(current_id) + 1
        p.multi()
        # Increment the message_id and use it to create a new message
        p.incr(MESSAGE_INDEX, 1)
        p.hset(MESSAGE_HASH, new_id, json.dumps(message))
        # Mark the message as enqueued
        p.sadd(ENQUEUED_MESSAGES_SET, new_id)
        # Push to the queue for processing
        p.lpush(MESSAGE_QUEUE, new_id)
        # Add the message to the sender's outbound list
        p.lpush(get_outbound_messages_list_name(message), new_id)
        # Notify the listener that it should call a worker to process a new message in the queue
        p.publish(MESSAGE_QUEUE_CHANNEL, new_id)

    # More on Redis transactions here: https://github.com/andymccurdy/redis-py/#pipelines
    message_id: int = r.transaction(create_msg_transaction, MESSAGE_INDEX)[0]
    return message_id


def process_enqueued_message(r: Redis) -> None:
    """ Pop the message from the top of the queue, check it for spam and deliver to the recipient. """
    message_id = int(r.lpop(MESSAGE_QUEUE))
    message: RawMessage = json.loads(r.hget(MESSAGE_HASH, message_id))
    # Mark the message as being checked for spam
    r.smove(ENQUEUED_MESSAGES_SET, BEING_SPAM_CHECKED_MESSAGES_SET, message_id)

    is_spam: bool = spam_check()

    if is_spam:
        # Mark the message as spam in Redis
        r.smove(BEING_SPAM_CHECKED_MESSAGES_SET, SPAM_MESSAGES_SET, message_id)
        # Make a record about spam in the event_journal
        r.publish(
            EVENT_JOURNAL_CHANNEL,
            f"SPAM: message with id {message_id} by {message['sender']}",
        )
        # Increment sender's score for spam messages
        r.zincrby(USERS_BY_SPAM_MESSAGES_SORTED_SET, 1, message["sender"])
    else:
        # Mark the message as inbound for the recipient
        r.lpush(get_inbound_messages_list_name(message), message_id)
        # Mark the message as delivered
        r.smove(BEING_SPAM_CHECKED_MESSAGES_SET, DELIVERED_MESSAGES_SET, message_id)
        # Increment sender's score for sent messages
        r.zincrby(USERS_BY_DELIVERED_MESSAGES_SORTED_SET, 1, message["sender"])


def get_outbound_messages_list_name(message: RawMessage):
    return f"{OUTBOUND_MESSAGES_LIST}:{message['sender']}"


def get_inbound_messages_list_name(message: RawMessage):
    return f"{INBOUND_MESSAGES_LIST}:{message['recipient']}"


def spam_check() -> bool:
    """ Imitate a spam check. """
    sleep(random.randrange(1, 3))
    return random.choice([True, False])
