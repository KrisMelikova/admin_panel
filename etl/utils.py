import time
from functools import wraps

from configs.logger import etl_logger


def backoff(
        start_sleep_time: float = 0.1,
        factor: int = 2,
        border_sleep_time: int = 15,
        max_amount_of_calls: int = 100,
):
    """
    The function tries to call an argument function after reconnect and delay if the argument
    function caused an exception.
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            for i in range(max_amount_of_calls):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    etl_logger.error(f"{exc}")

                    time_to_sleep = start_sleep_time * (factor ** i)
                    if time_to_sleep > border_sleep_time:
                        time_to_sleep = border_sleep_time

                    time.sleep(time_to_sleep)

        return inner

    return func_wrapper
