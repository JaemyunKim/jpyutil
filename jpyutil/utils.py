#-*- coding: utf-8 -*-

import os
import socket
import time
from datetime import datetime
from pathlib import Path
import pytz

import logging
from io import StringIO

from functools import wraps


# set timezone
tz_aware = pytz.timezone("Asia/Seoul")


def printLog(message, time_zone=pytz.timezone("UTC"), caller_name=None, out_messages=None):
    """print log message with time and caller's name

    Args:
        message (str): a message to print out
        time_zone (pytz.timezone, optional): time zone. Defaults to pytz.timezone("UTC").
        caller_name (str, optional): caller's name. Defaults to None.
        out_messages (list, optional): message list to receive caller's space. Defaults to None.
    """
    if caller_name is None:
        import inspect
        caller_name = inspect.currentframe().f_back.f_code.co_name

    dt = datetime.now(time_zone)
    msg = "{} [{}] {}".format(dt, caller_name, message)
    print(msg)

    if type(out_messages) is list:
        out_messages.append((dt, caller_name, message))


def get_logger(logger_name=None, log_level=logging.INFO):
    if logger_name is None:
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(logger_name)

    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.hasHandlers():
        # set for log on console
        log_format = "$(asctime)s.$(msecs)03d $(levelname)-8s [$(funcName)s] $(message)s"
        log_datefmt = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt=log_format, datefmt=log_datefmt)
        sh = logging.StreamHandler()
        sh.setLevel(log_level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


def logging_time(original_fn):
    @wraps(original_fn)
    def wrapper_fn(*args, **kwargs):
        start_time = time.time()
        result = original_fn(*args, **kwargs)
        end_time = time.time()
        print("WorkingTime [{}]: {} sec".format(original_fn.__name__, end_time - start_time))
        return result
    return wrapper_fn


def getWorkingMode(hostname_type):
    """select current working mode between dev and prd

    Args:
        hostname_type (dict): a dictionary consisted in {mode: hostname}

    Returns:
        str: working mode
    """
    cur_hname = socket.gethostname()
    printLog("current hostname: {}".format(cur_hname))

    try:
        #key_list = list(hostname_type.keys())
        #value_list = list(hostname_type.values())
        #working_mode = list(hostname_type.keys())[list(hostname_type.values()).index(cur_hanme)]
        working_mode = "local"
        for key, values in hostname_type.items():
            for v in values:
                if v in cur_hname:
                    working_mode = key
                    break

    except Exception as e:
        msg = "current hostname ({}) is not correct target for working mode".format(cur_hname)
        printLog(msg, tz_aware)
        working_mode = "local"

    printLog("current working mode: {}".format(working_mode))

    return working_mode


def readEnv(path=".", filename="env"):
    fName = os.path.join(path, filename)
    try:
        with open(fName, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    except Exception as e:
        print(str(e))
        return False

    # split items and convert to dictionary.
    items = dict([line.split('=') for line in lines])

    del lines

    return items


def makeFilelist(directory="."):
    # check the target directory
    if not os.path.exists(directory):
        print("Cannot find the directory: {}".format(directory))
        return list()

    dt = datetime.now()
    directory = Path(directory)
    logFDir = Path("./logs")
    logFDir.mkdir(parents=True, exist_ok=True) #make dir
    logFName = Path(logFDir / "filelist.txt")

    # make a filename list
    print("making a filelist")
    try:
        with open(logFName, "w", encoding="utf-8") as f:
            f.write("filelist_ver=0.0.1\n")
            f.write("start_time={}\n".format(dt))
            f.write("directory-{}\n".format(directory))
            f.write("absolute_directory={}\n". format(directory.absolute()))
            f.write("filelist:\n")

    except EnvironmentError as e:
        print(str(e))
        return list()

    try:
        files = []
        for (path, dir, files_in_path) in os.walk(directory):
            if len(files_in_path) > 0:
                filenames = [os.path.join(path, filename).replace("\\", "/") for filename in files_in_path]
                files.extend(filenames)
                with open(logFName, "a", encoding="utf-8") as f:
                    f.write("\n".join(filenames))

        with open(logFName, "a", encoding="utf-8") as f:
            f.write("summary:\nnumber_of_files={}".format(len(files)))

    except EnvironmentError as e:
        print(str(e))
        return list()

    print("filelist has {} files".format(len(files)))

    return files
