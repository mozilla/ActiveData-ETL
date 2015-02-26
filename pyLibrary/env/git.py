from _subprocess import CREATE_NEW_PROCESS_GROUP
import subprocess
from pyLibrary.debugs.logs import Log


def get_git_revision():
    """
    GET THE CURRENT GIT REVISION
    """
    proc = subprocess.Popen(
        ["git", "log", "-1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        creationflags=CREATE_NEW_PROCESS_GROUP
    )

    try:
        while True:
            line = proc.stdout.readline().strip()
            if not line:
                continue
            if line.startswith("commit "):
                return line[7:]
    finally:
        try:
            proc.wait()
        except Exception:
            pass

