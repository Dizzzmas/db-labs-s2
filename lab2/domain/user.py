from redis import Redis

from domain.exceptions import (
    UsernameNotFoundException,
    AlreadyLoggedInException,
    NotLoggedInException,
)
from domain.redis_structures import (
    REGULAR_USERS_SET,
    ADMIN_USERS_SET,
    ONLINE_USERS_SET,
    EVENT_JOURNAL_CHANNEL,
)


def login_user(r: Redis, username: str) -> None:
    """Notify subscribers about the 'login' event.
    Make the user appear online, adding him to the "online" Redis set.
    """
    if username not in r.sunion(REGULAR_USERS_SET, ADMIN_USERS_SET):
        raise UsernameNotFoundException(username)
    elif r.sismember(ONLINE_USERS_SET, username):
        raise AlreadyLoggedInException(username)

    r.sadd(ONLINE_USERS_SET, username)
    r.publish(EVENT_JOURNAL_CHANNEL, f"LOGIN: {username}")


def logout_user(r: Redis, username: str) -> None:
    """Notify subscribers about the 'logout' event.
    Make the user appear offline, removing him from the "online" Redis set.
    """
    if username not in r.sunion(REGULAR_USERS_SET, ADMIN_USERS_SET):
        raise UsernameNotFoundException(username)
    elif not r.sismember(ONLINE_USERS_SET, username):
        raise NotLoggedInException(username)

    # Make the user appear offline
    r.srem(ONLINE_USERS_SET, username)
    # Notify subscribers about the 'logout' event
    r.publish(EVENT_JOURNAL_CHANNEL, f"LOGOUT: {username}")
