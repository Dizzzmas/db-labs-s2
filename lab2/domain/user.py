from redis import Redis

from domain.exceptions import (
    UsernameNotFoundException,
    AlreadyLoggedInException,
    NotLoggedInException,
)
from domain.pub_sub_listeners import EVENT_JOURNAL_CHANNEL

REGULAR_USERS_SET = "regular_users"
ADMIN_USERS_SET = "admin_users"
ONLINE_USERS_SET = "online_users"


def login_user(r: Redis, username: str) -> None:
    """Notify subscribers about the 'login' event.
    Make the user appear online, adding him to the "online" Redis set.
    """
    if username not in r.sunion(REGULAR_USERS_SET, ADMIN_USERS_SET):
        raise UsernameNotFoundException(username)
    elif r.sismember(ONLINE_USERS_SET, username):
        raise AlreadyLoggedInException(username)

    r.publish(EVENT_JOURNAL_CHANNEL, f"{username} has logged in.")
    r.sadd(ONLINE_USERS_SET, username)


def logout_user(r: Redis, username: str) -> None:
    """Notify subscribers about the 'logout' event.
    Make the user appear offline, removing him from the "online" Redis set.
    """
    if username not in r.sunion(REGULAR_USERS_SET, ADMIN_USERS_SET):
        raise UsernameNotFoundException(username)
        abort(404, message=f"Couldn't find user with username {username}")
    elif not r.sismember(ONLINE_USERS_SET, username):
        raise NotLoggedInException(username)
        abort(418, message=f"Can't logout a user that's not logged in.")

    # Notify subscribers about the 'logout' event
    r.publish(EVENT_JOURNAL_CHANNEL, f"{username} has logged out.")
    # Make the user appear offline
    r.srem(ONLINE_USERS_SET, username)
