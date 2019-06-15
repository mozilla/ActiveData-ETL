print("start import timing")


from time import time

from mo_future import __builtin__

# INSPIRED BY https://stackoverflow.com/a/38407288/7202445


old_import = __builtin__.__import__

indent = 0


def __import__(*args, **kwargs):
    global indent

    indent += 1
    name = args[0]
    start = time()
    try:
        return old_import(*args, **kwargs)
    finally:
        end = time()
        indent -= 1
        ms = 1000 * (end - start)
        if ms > 1:
            print(
                ("  " * indent)
                + "time to import "
                + name
                + ": "
                + str(round(ms))
                + " ms"
            )


__builtin__.__import__ = __import__
