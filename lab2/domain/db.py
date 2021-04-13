from redis import Redis

from domain.user import REGULAR_USERS_SET, ADMIN_USERS_SET

REGULAR_USERS = ["Alice", "Malory"]
ADMIN_USERS = ["Dizzzmas", "Ilya"]


def seed_db(r: Redis):
    r.sadd(REGULAR_USERS_SET, *REGULAR_USERS)
    r.sadd(ADMIN_USERS_SET, *ADMIN_USERS)
