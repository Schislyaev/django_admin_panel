import time


# looping decorator
def sleep(timeout):
    def the_real_decorator(function):
        def wrapper(*args, **kwargs):
            while True:
                function(*args, **kwargs)
                time.sleep(timeout)
        return wrapper
    return the_real_decorator

