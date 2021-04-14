import json
from typing import List

import redis
from flask import Flask, request
from flask_smorest import abort

from domain.db import seed_db, start_listeners
from domain.exceptions import (
    UsernameNotFoundException,
    AlreadyLoggedInException,
    NotLoggedInException,
)
from domain.message import (
    create_message,
    RawMessage,
    Message,
    MessageDeliveryStatus,
    get_inbound_messages_list_name,
    get_outbound_messages_list_name,
)
from domain.redis_structures import (
    INBOUND_MESSAGES_SET,
    DELIVERED_MESSAGES_SET,
    MESSAGE_HASH,
    ENQUEUED_MESSAGES_SET,
    SPAM_MESSAGES_SET,
    BEING_SPAM_CHECKED_MESSAGES_SET,
    USERS_BY_SPAM_MESSAGES_SORTED_SET,
    USERS_BY_DELIVERED_MESSAGES_SORTED_SET,
    ONLINE_USERS_SET,
    EVENT_JOURNAL_LIST,
)
from domain.user import login_user, logout_user

app = Flask(__name__)

app.secret_key = "asdf"
# Create a connection instance to redis.
r = redis.Redis("127.0.0.1", decode_responses=True)

seed_db(r)


@app.route("/login", methods=["POST"])
def login():
    if not request.json.get("username"):
        abort(422, message="Missing 'username' in the request body")
    username = request.json["username"]

    try:
        login_user(r, username)
    except UsernameNotFoundException as exc:
        abort(404, message=str(exc))
    except AlreadyLoggedInException as exc:
        abort(418, message=str(exc))

    return "Logged in."


@app.route("/logout", methods=["POST"])
def logout():
    if not request.json.get("username"):
        abort(422, message="Missing 'username' in the request body")
    username: str = request.json["username"]

    try:
        logout_user(r, username)
    except UsernameNotFoundException as exc:
        abort(404, message=str(exc))
    except NotLoggedInException as exc:
        abort(418, message=str(exc))

    return "Logged out."


@app.route("/message", methods=["POST"])
def send_message() -> Message:
    if (
        not request.json.get("sender")
        or not request.json.get("recipient")
        or not request.json.get("content")
    ):
        abort(422, message="Missing field in request body")
    sender: str = request.json["sender"]
    recipient: str = request.json["recipient"]
    content: str = request.json["content"]
    message: RawMessage = dict(sender=sender, recipient=recipient, content=content)

    message_id: int = create_message(r, message)
    return dict(id=message_id, **message)


@app.route("/inbound-messages", methods=["GET"])
def get_inbound_messages():
    """ Get messages received by the user. """
    username: str = request.args.get("username")

    inbound_message_ids = r.sinter(
        get_inbound_messages_list_name(username), DELIVERED_MESSAGES_SET
    )
    inbound_messages = r.hmget(MESSAGE_HASH, *inbound_message_ids)
    response: List[Message] = []
    for message_id, message in zip(inbound_message_ids, inbound_messages):
        message = json.loads(message)
        message["id"] = int(message_id)
        response.append(message)

    return dict(messages=response)


@app.route("/user-stats", methods=["GET"])
def get_message_stats():
    """ Get user's messages by status. """
    username: str = request.args.get("username")
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
        being_spam_checked_count=being_spam_checked_count,
    )


@app.route("/spammer-stats", methods=["GET"])
def get_spammer_stats():
    """ Get most spammy users in a descending order. """
    spammers = r.zrange(USERS_BY_SPAM_MESSAGES_SORTED_SET, 0, -1, withscores=True)
    spammers = spammers[::-1]

    return dict(spammers=spammers)


@app.route("/online-users", methods=["GET"])
def get_online_users():
    """ Get a list of online users. """
    online_users = list(r.smembers(ONLINE_USERS_SET))

    return dict(online_users=online_users)


@app.route("/chatter-stats", methods=["GET"])
def get_highest_activity_stats():
    """ Get most active users in a descending order. """
    chatters = r.zrange(USERS_BY_DELIVERED_MESSAGES_SORTED_SET, 0, -1, withscores=True)
    chatters = chatters[::-1]

    return dict(chatters=chatters)


@app.route("/event-journal", methods=["GET"])
def get_event_journal():
    """ Get a chronological event log. """
    events = r.lrange(EVENT_JOURNAL_LIST, 0, -1)

    return dict(chatters=events)


if __name__ == "__main__":
    start_listeners(r)
    app.run()
