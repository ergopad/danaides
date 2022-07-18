import logging 
import time

from colorlog import ColoredFormatter, StreamHandler, getLogger

LEIF = 5
logging.addLevelName(LEIF, 'LEIF')
formatter = ColoredFormatter(
    # {color}, fg_{color}, bg_{color}: Foreground and background colors.
    # bold, bold_{color}, fg_bold_{color}, bg_bold_{color}: Bold/bright colors.
    # thin, thin_{color}, fg_thin_{color}: Thin colors (terminal dependent).
    # reset: Clear all formatting (both foreground and background colors).
	'%(log_color)s%(levelname)s%(reset)s:%(asctime)s:%(purple)s%(name)s%(reset)s:%(log_color)s%(message)s%(reset)s',
	datefmt=None,
	reset=True,
    # black, red, green, yellow, blue, purple, cyan, white
	log_colors={
		'DEBUG':    'cyan',
		'INFO':     'green',
		'WARNING':  'yellow',
		'ERROR':    'red',
		'CRITICAL': 'red,bg_white',
        'LEIF':     'white,bg_green'
	},
	secondary_log_colors={},
	style='%'
)
handler = StreamHandler()
handler.setFormatter(formatter)
logger = getLogger('ergopad')
logger.setLevel('LEIF')
logger.addHandler(handler)

# prevent logging from other handlers
getLogger('uvicorn.error').propagate = False
getLogger('sqlalchemy.engine.Engine').propagate = False
getLogger('sqlalchemy.pool.impl.QueuePool').propagate = False
getLogger('ergopad').propagate = False

import inspect
myself = lambda: inspect.stack()[1][3]

# stopwatch
class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

class Timer:
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        return elapsed_time

    def split(self):
        """check current elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        return f"{elapsed_time:0.4f}s"

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()