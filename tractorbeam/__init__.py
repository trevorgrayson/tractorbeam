from importlib import import_module

from .sources import source
from .sinks import sink


def get_pipe(s: str):
    parts = s.split(".")
    print(".".join(parts[0:-1]))
    return getattr(import_module(".".join(parts[0:-1])), parts[-1])
