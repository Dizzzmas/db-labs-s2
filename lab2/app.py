from typing import List, Dict

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
    UserMessagingStats,
    fetch_messaging_stats_for_user,
    fetch_most_spamming_users,
    fetch_online_users,
    fetch_user_inbound_messages,
    fetch_highest_activity_stats,
    fetch_event_journal,
)
from domain.user import login_user, logout_user

app = Flask(__name__)

app.secret_key = "not_safe"
# Create a connection instance to redis.
r = redis.Redis("127.0.0.1", decode_responses=True)

r.flushall()
seed_db(r)


@app.route("/login", methods=["POST"])
def login():
    if not request.json.get("username"):
        abort(422, message="Missing 'username' in the request body")
    username: str = request.json["username"]

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
    inbound_messages: List[Message] = fetch_user_inbound_messages(r, username)
    return dict(inbound_messages=inbound_messages)


@app.route("/user-stats", methods=["GET"])
def get_message_stats():
    """ Get user's messages by status. """
    username: str = request.args.get("username")
    messaging_stats: UserMessagingStats = fetch_messaging_stats_for_user(r, username)
    return messaging_stats


@app.route("/spammer-stats", methods=["GET"])
def get_spammer_stats():
    """ Get most spammy users in a descending order. """
    spammers: List[Dict[str, int]] = fetch_most_spamming_users(r)
    return dict(spammers=spammers)


@app.route("/online-users", methods=["GET"])
def get_online_users():
    """ Get a list of online users. """
    online_users: List[str] = fetch_online_users(r)
    return dict(online_users=online_users)


@app.route("/chatter-stats", methods=["GET"])
def get_highest_messaging_activity_stats():
    """ Get users with most delivered messages in a descending order. """
    chatters: List[Dict[str, int]] = fetch_highest_activity_stats(r)
    return dict(chatters=chatters)


@app.route("/event-journal", methods=["GET"])
def get_event_journal():
    """ Get a chronological event log. """
    events: List[str] = fetch_event_journal(r)
    return dict(events=events)


if __name__ == "__main__":
    start_listeners(r)
    app.run()
