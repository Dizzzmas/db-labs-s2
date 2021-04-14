class UsernameException(Exception):
    username: str

    def __init__(self, username):
        super().__init__()
        self.username = username


class UsernameNotFoundException(UsernameException):
    def __str__(self):
        return f"Couldn't find user with username {self.username}"


class AlreadyLoggedInException(UsernameException):
    def __str__(self):
        return f"User {self.username} is already logged in."


class NotLoggedInException(UsernameException):
    def __str__(self):
        return f"User {self.username} isn't logged in."
