"""Set up logger to be used across library."""
import logging

logger = logging.getLogger('py4phi_logger')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S'
)

ch.setFormatter(formatter)

logger.addHandler(ch)
