#-*- coding: utf-8 -*-

from jpyutil.utils import readEnv
from jpyutil.S3Storage import S3Storage
import multiprocessing


def main():
	# read and set variables for s3 storage
	env_items = readEnv(filename=".env")
	env_items["BUCKET_NAME"] = "dataset" # S3 bucket name
	env_items["PREFIX"] = "db_prefix/db_dataset"  # destination path
	env_items["LOCAL_DIRECTORY"] = "./test" # source directory
	env_items["NUM_PROCESS"] = 1 * multiprocessing.cpu_count() - 1

        log_dir = "logs"

	# generate a filename list and upload them
	s3_storage = S3Storage(env_items, log_dir=log_dir)
	if s3_storage.upload_files() is False:
            print("Failed to run S3Storage!!")
            exit(1)


if __name__ == "__main__":
	main()
