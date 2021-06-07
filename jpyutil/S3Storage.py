#-*- coding: utf-8 -*-

# logger setting
import logging
from .utils import get_logger
logger = get_logger(logger_name=__name__, log_level=logging.INFO)

import os
import math
import time
from datetime import datetime
from pathlib import Path
import tqdm
import multiprocessing

import boto3
from boto3.s3.transfer import TransferConfig
import botocore
from botocore.config import Config
boto3.set_stream_logger(name='boto3', level=logging.WARNING)
boto3.set_stream_logger(name='botocore', level=logging.WARNING)


class S3Storage():
    def __init__(self, env_items, log_dir="logs"):
        if "NUM_PROCESS" in env_items.keys():
            if env_items["NUM_PROCESS"] == 1:
                self.process_count = 1
            elif env_items["NUM_PROCESS"] > 1:
                # self.process_count = max(min(env_items["NUM_PROCESS"], multiprocessing.cpu_count()), 1)
                self.process_count = env_items["NUM_PROCESS"]
            else:
                self.process_count = multiprocessing.cpu_count()
        else:
            self.process_count = 1

        self.env_items = env_items
        self.log_dir = Path(log_dir)
        self.file_names = list()
        slef.manager = multiprocessing.Manager()
        self.transfer_result = self.manager.dict()
        self.transfer_result["ok"] = self.manager.list()
        self.transfer_result["fail"] = self.manager.list()
        self.transfer_result["miss"] = self.manager.list()

        # generate a file list
        self.__getFilelist()


     def getFilelist(self):
        # check the target directory or the target file
        target = Path(self.env_items["LOCAL_DIRECTORY"])
        if not target.exists():
            print("Cannot find the target: {}".format(target))
            self.file_names.clear()
            return False

        dt = datetime.now()
        logFDir = Path(log_dir)
        logFDir.mkdir(parents=True, exist_ok=True) # make dir
        logFName = Path(logFDir / "filelist.txt")

        self.file_names.clear()
        if target.is_dir():
            directory = target
            try:
                with open(logFName, "w", encoding="utf-8") as f:
                    f.write("filelist_ver=0.0.1\n")
                    f.write("start_time={}\n".format(dt))
                    f.write("directory={}\n".format(target))
                    f.write("absolute_directory={}\n".format(target.absolute()))
                    f.write("filelist:\n")

            except Exception as e:
                print(str(e))
                self.file_names.clear()
                return False

            try:
                for (path, dir, files_in_path) in os.walk(directory):
                    if len(files_in_path) > 0:
                        filenames = [os.path.join(path, filename).replace("\\", "/") for filename in files_in_path]
                        self.file_names.extend(filenames)
                        with open(logFName, "a", encoding="utf-8") as f:
                            f.write("\n".join(filenames))
                            f.write("\n")

                with open(logFName, "a", encoding="utf-8") as f:
                    f.write("summary:\nnumber_of_files={}".format(len(files)))

            except Exception as e:
                print(str(e))
                self.file_names.clear()
                return False

        elif target.is_file():
            try:
                fName = target
                with open(fName, "r", encoding="utf-8") as f:
                    lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

                idx_begin, idx_end = 0, len(lines)
                for i in range(len(lines)):
                    if lines[i] == "filelist:":
                        idx_begin = i + 1
                    if lines[i] == "summary:":
                        idx_end = i
                        break

                self.file_names = lines[idx_begin:idx_end]
                print("# of files in {}: {}".format(fName, len(self.file_names)))

            except Exception as e:
                print(str(e))
                self.file_names.clear()
                return False

            try:
                directory = target.parent
                log_pid_list = []
                target_fName = "log-file_upload"
                for (path, dir, files_in_path) in os.walk(directory):
                    for log_fName in files_in_path:
                        if log_fName[:len(target_fName)] == target_fName:
                            log_pid_list.append(log_fName)

                files_set = set(self.file_names)
                for log_pid_fName in log_pid_list:
                    with open(os.path.join(directory, log_pid_fName), "r", encoding="utf-8") as f:
                        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

                    idx_begin, idx_end = 0, len(lines)
                    for i in range(len(lines)):
                        if lines[i] == "filelist:":
                            idx_begin = i + 1
                        if lines[i] == "summary:":
                            idx_end = i
                            break

                    transfer_files = [line.split("\t")[1:3][1] for line in lines[idx_begin:idx_end] if line.split("\t")[1:3][0][13:] == "ok"]
                    files_set = files_set - set(transfer_files)

                self.file_names = list(files_set)

                with open(logFName, "w", encoding="utf-8") as f:
                    # write header
                    for line in lines[:idx_begin]:
                        if "start_time" in line.split("=")[0]:
                            f.write("start_time={}\n".format(dt))
                        else:
                            f.write("{}\n".format(line))

                    # write contents
                    f.write("\n".join(self.file_names))
                    f.write("\n")

                    # write trailer
                    f.write("summary:\nnumber_of_files={}".format(len(self.file_names)))

            except Exception as e:
                print(str(e))
                self.file_names.clear()
                return False

        print("filelist has {} files".format(len(self.file_names)))

        return True


    def upload_files(self, file_names):
        if len(self.file_names) == 0:
            return False

        # divide the list to n lists
        item_nb = math.ceil(len(self.file_names) / self.process_count)
        tasks = [self.file_names[i:i + item_nb] for i in range(0, len(self.file_names), item_nb)]

        #with tqdm.tqdm(total = len(self.file_names)) as global_progress:
            #global_progress.set_description("total")
        mps = list()
        for i in range(len(tasks)):
            worker = Worker_S3Uploader(self.transfer_result, self.env_items, tasks[i], log_dir=self.log_dir)#, global_progress)
            worker.daemon = True
            mps.append(worker)

        print("{} files, ready to start!!\n".format(len(self.file_names)))

        # run multiprocessing.Process
        for mp in mps:
            mp.start()

        # joining the process
        for mp in mps:
            mp.join()

        # get a report
        transfer_ok = self.transfer_result["ok"]
        transfer_fail = self.transfer_result["fail"]
        transfer_miss = self.transfer_result["miss"]

        print("\nupload done")
        print("total files: {}".format(len(self.file_names)))
        print("success: {}\t failure: {}".format(len(self.transfer_ok), len(self.transfer_fail)))

        self.transfer_miss = list(set(self.file_names) - set(self.transfer_ok) - set(self.transfer_fail))
        if len(self.transfer_miss) > 0:
            print("missing: {}".format(len(self.transfer_miss)))
        else:
            print("no missing files")

        return True


    def download_files(self, env_items, file_names):
        pass


class Worker_S3Uploader(multiprocessing.Process):
    def __init__(self, transfer_result, env_items, file_list, log_dir = "logs", global_tqdm = None):
        multiprocessing.Process.__init__(self)
        self.exit = multiprocessing.Event()

        self.result = transfer_result

        self.env_items = env_items
        self.file_list = file_list
        self.global_tqdm = global_tqdm
        self.logFDir = Path(log_dir)

        self.transfer_ok = []
        self.transfer_failed = []

        # Enable thread use/transfer concurrency
        GB = 1024 ** 3
        self.config = TransferConfig(
            multipart_threshold=100 * GB, 
            max_concurrency=30,
            use_threads=True
        )


    def report(self):
        return self.transfer_ok, self.transfer_failed

    def run(self):
        try:
            self.client = boto3.client(
                service_name=self.env_items["S3_SERVICE_NAME"], 
                region_name=self.env_items["S3_REGION_NAME"], 
                aws_access_key_id=self.env_items["AWS_ACCESS_KEY_ID"], 
                aws_secret_access_key=self.env_items["AWS_SECRET_ACCESS_KEY"], 
                endpoint_url=self.env_items["S3_ENDPOINT_URL"]
            )
        except Exception as e:
            print(e)
            return

        # generate an uploaded filelist
        dt = datetime.now()
        directory = Path(self.env_items["LOCAL_DIRECTORY"])
        self.logFDir.mkdir(parents=True, exist_ok=True) # make dir
        logFName = Path(self.logFDir / "log-file_upload-{}.txt".format(self.pid))
        with open(logFName, "w", encoding="utf-8") as f:
            f.write("filelist_ver=0.0.1\n")
            f.write("start_time={}\n".format(dt))
            f.write("directory={}\n".format(directory))
            f.write("absolute_directory={}\n".format(directory.absolute()))
            f.write("s3_bucket={}\n".format(self.env_items["BUCKET_NAME"]))
            f.write("s3_prefix={}\n".format(self.env_items["PREFIX"]))
            f.write("number_of_files={}\n".format(len(self.file_list)))
            f.write("filelist:\n")

        try:
            for item in self.file_list:
                srcFName = item
                dstFName = os.path.relpath(srcFName, start=self.env_items["LOCAL_DIRECTORY"])
                dstFName = os.path.join(self.env_items["PREFIX"], dstFName.replace(" ", "_")).replace("\\", "/")

                try:
                    self.client.upload_file(srcFName, self.env_items["BUCKET_NAME"], dstFName, Config=self.config)
                    self.result["ok"].append(srcFName)
                    msg_log = "ok\t{}\t->\t{}".format(srcFName, dstFName)
                    msg_print = "ok\t{}".format(dstFName)

                except:# Exception as e: 
                    self.result["fail"].append(srcFName)
                    msg_log = "failed\t{}\t->\t{}".format(srcFName, dstFName)
                    msg_print = "failed\t{}".format(dstFName)

                finally:
                    dt = datetime.now()
                    msg = "{}\tpid: {:<8}".format(dt, self.pid)
                    msg_log = "{}{}".format(msg, msg_log)
                    msg_print = "{}{}".format(msg, msg_print)

                with open(logFName, "a", encoding = "utf-8") as f:
                        f.write("{}\n".format(msg_log))
                print(msg_print)

                if self.global_tqdm is not None:
                    self.global_tqdm.update()

            with open(logFName, "a", encoding="utf-8") as f:
                f.write("summary:\nsuccess={}\nfailed={}".format(len(self.result["ok"]), len(self.result["fail"])))

        except Exception as e:
            print(e)
            return


    # to terminate uploader process
    def shutdown(self):
        print("Shutdown pid={}".format(self.pid))
        self.exit.set()
