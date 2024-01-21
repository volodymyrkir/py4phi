"""
This module provides base class logic related to reading data.
"""

class BaseReader:

    def __init__(self, processor: str = 'PySpark'):
        self._processor = processor
