import itertools
import string
from typing import Generator


def gen_prefixes(length: int = 2, use_uppercase: bool = False) -> Generator[str, None, None]:
    charset = string.ascii_lowercase + string.digits
    if use_uppercase:
        charset += string.ascii_uppercase
    yield from ["".join(pair) for pair in itertools.product(charset, repeat=length)]
