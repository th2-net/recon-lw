import importlib
import pkgutil
import traceback
from pathlib import Path


def test_import_plugins():
    root = Path("../recon_lw").resolve()
    stack = [root]
    while stack:
        current = stack.pop()
        for _, name, ispkg in pkgutil.iter_modules([str(current)]):
            relative = str(current.relative_to(root)).replace("\\", ".")
            if relative != ".":
                relative = f".{relative}."
            if ispkg:
                stack.append(current / name)
                try:
                    importlib.import_module(f"recon_lw{relative}{'__init__'}")
                except ImportError:
                    print(traceback.format_exc())
                    raise
            else:
                try:
                    importlib.import_module(f"recon_lw{relative}{name}")
                except ImportError:
                    print(traceback.format_exc())
                    raise
