import redis
from flask import Flask, request
from flask_smorest import abort

from domain.db import seed_db, start_listeners
from domain.exceptions import (
    UsernameNotFoundException,
    AlreadyLoggedInException,
    NotLoggedInException,
)
from domain.message import create_message, RawMessage, Message
from domain.user import login_user, logout_user

app = Flask(__name__)

app.secret_key = "asdf"
# Create a connection instance to redis.
r = redis.Redis("127.0.0.1", decode_responses=True)


seed_db(r)


@app.route("/")
def hello():
    return r.get("admin_users")


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


if __name__ == "__main__":
    start_listeners(r)
    app.run()
