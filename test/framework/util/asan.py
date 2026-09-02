from typing import Dict
import os


def parse_asan_options(options: str) -> Dict[str, str]:
    """
    Parses an `ASAN_OPTIONS`-style string the same way the LLVM
    sanitizer runtime does
    (compiler-rt/lib/sanitizer_common/sanitizer_flag_parser.cpp).

    The LLVM parser treats space, comma, colon, newline, tab and
    carriage return as flag separators and supports quoting with
    ' and ".
    """

    def is_separator(c):
        return c in (" ", ",", ":", "\n", "\t", "\r")

    result = {}
    pos = 0
    n = len(options)
    while True:
        # skip_whitespace
        while pos < n and is_separator(options[pos]):
            pos += 1
        if pos >= n:
            break
        # parse_flag: read name up to '=' or a separator
        name_start = pos
        while pos < n and options[pos] != "=" and not is_separator(options[pos]):
            pos += 1
        # The LLVM parser treats a missing '=' as a fatal error; we
        # just stop parsing instead of raising in the test harness.
        if pos >= n or options[pos] != "=":
            break
        name = options[name_start:pos]
        pos += 1  # consume '='
        # parse_flag: read value
        if pos < n and options[pos] in ("'", '"'):
            quote = options[pos]
            pos += 1  # consume opening quote
            value_start = pos
            while pos < n and options[pos] != quote:
                pos += 1
            if pos >= n:
                # unterminated quoted string; stop parsing
                break
            value = options[value_start:pos]
            pos += 1  # consume closing quote
        else:
            value_start = pos
            while pos < n and not is_separator(options[pos]):
                pos += 1
            value = options[value_start:pos]
        result[name] = value
    return result


def get_configured_asan_exit_code() -> int | None:
    asan_options = os.environ.get("ASAN_OPTIONS")
    if not asan_options:
        return None
    asan_options_dict = parse_asan_options(asan_options)
    exitcode = asan_options_dict.get("exitcode")
    if exitcode is not None:
        return int(exitcode)
    else:
        return None
