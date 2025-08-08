from typing import Generator
import itertools
import string
import pkgutil
import importlib


def gen_prefixes(length: int = 2, use_uppercase: bool = False) -> Generator[str, None, None]:
    charset = string.ascii_lowercase + string.digits
    if use_uppercase:
        charset += string.ascii_uppercase
    yield from ["".join(pair) for pair in itertools.product(charset, repeat=length)]


def register_import_modules(name: str, module_path: str):
    for _, module_name, is_pkg in pkgutil.iter_modules(module_path):
        if is_pkg and module_name not in ("registry",):
            importlib.import_module(f"{name}.{module_name}")


def split_and_strip(string: str, separator: str = ",") -> list[str]:
    return [item.strip() for item in string.split(separator) if item.strip()]
