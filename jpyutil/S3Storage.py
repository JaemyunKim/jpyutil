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
        if "NUM_CPUS" in env_items.keys():
            if env_items["NUM_CPUS"] == 1:
                self.process_count = 1
            elif env_items["NUM_CPUS"] > 1:
                # self.process_count = max(min(env_items["NUM_CPUS"], multiprocessing.cpu_count()), 1)
                self.process_count = env_items["NUM_CPUS"]
            else:
                self.process_count = multiprocessing.cpu_count()
        else:
            self.process_count = 1

        self.env_items = env_items
        self.log_dir = log_dir
        self.file_names = list()


    def upload_files(self, file_names):
        self.file_names = file_names

        # divide the list to n lists
        item_nb = math.ceil(len(self.file_names) / self.process_count)
        tasks = [self.file_names[i:i + item_nb] for i in range(0, len(self.file_names), item_nb)]

        #with tqdm.tqdm(total = len(self.file_names)) as global_progress:
            #global_progress.set_description("total")
        mps = list()
        for i in range(len(tasks)):
            worker = Worker_S3Uploader(self.env_items, tasks[i], log_dir=self.log_dir)#, global_progress)
            worker.daemon = True
            mps.append(worker)

        print("{} files, ready to start!!\n".format(len(self.file_names)))

        # run multiprocessing.Process
        for mp in mps:
            mp.start()

        # joining the process
        for mp in mps:
            mp.join()

        print("\nupload done")


    def download_files(self, env_items, file_names):
        pass


    def run(self):
        pass


class Worker_S3Uploader(multiprocessing.Process):
    def __init__(self, env_items, file_list, log_dir = "logs", global_tqdm = None):
        multiprocessing.Process.__init__(self)
        self.exit = multiprocessing.Event()

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
            max_concurrency=10,
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
            return False

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
                    self.transfer_ok.append(srcFName)
                    msg_log = "ok\t{}\t->\t{}".format(srcFName, dstFName)
                    msg_print = "ok\t{}".format(dstFName)

                except:# Exception as e: 
                    self.transfer_failed.append(srcFName)
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
                f.write("summary:\nsuccess={}\nfailed={}".format(len(self.transfer_ok), len(self.transfer_failed)))

        except Exception as e:
            print(e)
            return False

        return True


    # to terminate uploader process
    def shutdown(self):
        print("Shutdown pid={}".format(self.pid))
        self.exit.set()
