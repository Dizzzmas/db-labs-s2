import json
from enum import Enum, unique
from typing import TypedDict, List, Dict

from redis.client import Pipeline, Redis
import random
from time import sleep

from domain.redis_structures import (
    MESSAGE_QUEUE_CHANNEL,
    MESSAGE_QUEUE,
    ENQUEUED_MESSAGES_SET,
    MESSAGE_HASH,
    MESSAGE_INDEX,
    OUTBOUND_MESSAGES_SET,
    BEING_SPAM_CHECKED_MESSAGES_SET,
    SPAM_MESSAGES_SET,
    EVENT_JOURNAL_CHANNEL,
    USERS_BY_SPAM_MESSAGES_SORTED_SET,
    DELIVERED_MESSAGES_SET,
    USERS_BY_DELIVERED_MESSAGES_SORTED_SET,
    INBOUND_MESSAGES_SET,
    ONLINE_USERS_SET,
    EVENT_JOURNAL_LIST,
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


class UserMessagingStats(TypedDict):
    """
    Stats representing suer's messaging activity.
    """

    delivered: int
    enqueued: int
    marked_as_spam: int
    being_spam_checked: int


def create_message(r, message: RawMessage) -> int:
    """ Create a new message in Redis Hash and enqueue it for spam detection an eventual delivery. """

    def create_msg_transaction(p: Pipeline):
        current_id = p.get(MESSAGE_INDEX)
        new_id = (
            int(current_id) + 1 if current_id else 1
        )  # If id doesn't exist in redis, set it to 1
        p.multi()
        # Increment the message_id and use it to create a new message
        p.incr(MESSAGE_INDEX, 1)
        p.hset(MESSAGE_HASH, new_id, json.dumps(message))
        # Mark the message as enqueued
        p.sadd(ENQUEUED_MESSAGES_SET, new_id)
        # Push to the queue for processing
        p.rpush(MESSAGE_QUEUE, new_id)
        # Add the message to the sender's outbound list
        p.sadd(get_outbound_messages_list_name(message["sender"]), new_id)
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
        r.sadd(get_inbound_messages_list_name(message["recipient"]), message_id)
        # Mark the message as delivered
        r.smove(BEING_SPAM_CHECKED_MESSAGES_SET, DELIVERED_MESSAGES_SET, message_id)
        # Increment sender's score for sent messages
        r.zincrby(USERS_BY_DELIVERED_MESSAGES_SORTED_SET, 1, message["sender"])


def fetch_user_inbound_messages(r: Redis, username: str) -> List[Message]:
    """ Get messages received by the user with "username" """
    inbound_message_ids = r.sinter(
        get_inbound_messages_list_name(username), DELIVERED_MESSAGES_SET
    )
    if not inbound_message_ids:
        return []

    inbound_messages = r.hmget(MESSAGE_HASH, *inbound_message_ids)
    messages_with_ids: List[Message] = []
    for message_id, message in zip(inbound_message_ids, inbound_messages):
        message = json.loads(message)
        message["id"] = int(message_id)
        messages_with_ids.append(message)

    return messages_with_ids


def fetch_messaging_stats_for_user(r: Redis, username: str) -> UserMessagingStats:
    """ View how many of user's messages are at the moment enqueued/being spam checked/marked as spam/delivered. """
    delivered_messages_count = len(
        r.sinter(get_outbound_messages_list_name(username), DELIVERED_MESSAGES_SET)
    )
    enqueued_messages_count = len(
        r.sinter(get_outbound_messages_list_name(username), ENQUEUED_MESSAGES_SET)
    )
    marked_as_spam_count = len(
        r.sinter(get_outbound_messages_list_name(username), SPAM_MESSAGES_SET)
    )
    being_spam_checked_count = len(
        r.sinter(
            get_outbound_messages_list_name(username), BEING_SPAM_CHECKED_MESSAGES_SET
        )
    )

    return dict(
        delivered=delivered_messages_count,
        enqueued=enqueued_messages_count,
        marked_as_spam=marked_as_spam_count,
        being_spam_checked=being_spam_checked_count,
    )


def fetch_most_spamming_users(r: Redis) -> List[Dict[str, int]]:
    """ Get most spammy users in a descending order. """
    spammers = r.zrange(USERS_BY_SPAM_MESSAGES_SORTED_SET, 0, -1, withscores=True)
    spammers = spammers[::-1]
    return spammers


def fetch_highest_activity_stats(r: Redis) -> List[Dict[str, int]]:
    """ Get users with most delivered messages in a descending order. """
    chatters = r.zrange(USERS_BY_DELIVERED_MESSAGES_SORTED_SET, 0, -1, withscores=True)
    chatters = chatters[::-1]

    return chatters


def fetch_event_journal(r: Redis) -> List[str]:
    """ Get a list of events for sign in/sign out/ message being marked as spam. """
    events = r.lrange(EVENT_JOURNAL_LIST, 0, -1)
    return events


def fetch_online_users(r: Redis) -> List[str]:
    """ Get a list of online users. """
    online_users = list(r.smembers(ONLINE_USERS_SET))
    return online_users


def get_outbound_messages_list_name(sender: str):
    return f"{OUTBOUND_MESSAGES_SET}:{sender}"


def get_inbound_messages_list_name(recipient: str):
    return f"{INBOUND_MESSAGES_SET}:{recipient}"


def spam_check() -> bool:
    """ Imitate a spam check. """
    sleep(random.randrange(1, 3))
    return random.choice([True, False])
