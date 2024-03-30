"""Module containing common utility functions for CLI."""


def parse_key_value(*args):
    """Parse read/write key-value boolean parameters."""
    opts = dict(args[-1])
    d = {
        key: val.lower() == 'true'
        if val.lower() in ('true', 'false') else val
        for key, val in opts.items()
    }
    return d
