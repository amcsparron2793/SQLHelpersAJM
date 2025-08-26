from getpass import getpass
import logging
import functools
import warnings
from backend.errors import InvalidInputMode

from backend.errors import MissingRequiredClassAttribute, NoTrackedTablesError, NoCursorInitializedError, \
    NoConnectionInitializedError, NoResultsToConvertError
from backend.meta import ABCCreateTriggers, ABCPostgresCreateTriggers


def deprecated(reason: str = ""):
    """
    Decorator that marks a function or method as deprecated.

    :param reason: Optional message to explain what to use instead
                   or when the feature will be removed.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            message = f"Function '{func.__name__}' is deprecated."
            if reason:
                message += f" {reason}"
            warnings.warn(message, category=DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    return decorator


class UserPassInput:
    """
    Class used to handle user and password input for database-related operations, providing prompts
    and logic for requesting and validating input. Supports modes for obtaining both username and
    password.

    Attributes:
        GET_PASS_PROMPT (str): Template for prompting the user to input a database password.
                               Formatted with the username.
        GET_USER_PROMPT (str): Template for prompting the user to input a database username.
                               Formatted with the database name.
        ALL_MODES (list): List of all supported input modes ('user', 'pass', 'password', 'username').
        USER_MODES (list): List of modes specifically for username input.
        PASSWORD_MODES (list): List of modes specifically for password input.

    Methods:
        get_user_pass(cls, **kwargs): Determines whether to prompt for username or password based on
                                      arguments provided, their presence, and connection type.
        _get_user_or_pass(cls, mode, **kwargs): Private method to handle the actual input collection,
                                                based on the input mode, and validates non-empty input.
    """
    GET_PASS_PROMPT = "Enter password for database user '{}' (no output will show on screen): "
    GET_USER_PROMPT = "Enter username for database '{}': "
    ALL_MODES = ['user', 'pass', 'password', 'username']
    USER_MODES = ['user', 'username']
    PASSWORD_MODES = ['pass', 'password']

    @classmethod
    def get_user_pass(cls, **kwargs):
        """
        :param kwargs: Arbitrary keyword arguments passed for retrieving user credentials.
        :type kwargs: dict
        :return: User or password details based on the provided arguments.
        :rtype: Any
        """
        mode = kwargs.pop('mode', None)
        username = kwargs.get('username', None)
        _password = kwargs.get('password', None)
        trusted_connection = kwargs.get('trusted_connection', None)
        untrusted_connection = not trusted_connection or trusted_connection == 'no'
        if mode is not None:
            logging.warning(f"mode in use, ignoring other kwargs forcing {mode}")
            return cls._get_user_or_pass(mode=mode, **kwargs)
        if not username and untrusted_connection:
            return cls._get_user_or_pass('user', **kwargs)
        if not _password and untrusted_connection:
            return cls._get_user_or_pass('pass', **kwargs)
        return None

    @classmethod
    def _get_user_or_pass(cls, mode, **kwargs):
        """
        :param mode: The mode indicating whether a username or password is being retrieved.
        :type mode: str
        :param kwargs: Additional arguments including optional `database` or `username` for prompt customization.
        :type kwargs: dict
        :return: The retrieved input from the user, either a username or password, based on the mode.
        :rtype: str
        :raises InvalidInputMode: If the provided mode is not recognized or valid.
        """
        database = kwargs.get('database', None)
        username = kwargs.get('username', None)

        user_prompt = cls.GET_USER_PROMPT.format(database)
        password_prompt = cls.GET_PASS_PROMPT.format(username)
        if mode not in cls.ALL_MODES:
            raise InvalidInputMode()
        try:
            while True:
                if mode in cls.PASSWORD_MODES:
                    prompt = getpass(password_prompt)
                elif mode in cls.USER_MODES:
                    prompt = input(user_prompt)
                else:
                    raise InvalidInputMode()

                if prompt:
                    break
                if prompt.lower() == 'q':
                    print("quitting, goodbye")
                    exit()
                else:
                    logging.warning(f"{mode} cannot be empty")
            return prompt
        except KeyboardInterrupt:
            print("quitting, goodbye")
            exit()
