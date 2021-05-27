#-*- coding: utf-8 -*-

from jpyutil.utils import readEnv, makeFilelist
from jpyutil.S3Storage import S3Storage
import multiprocessing


def main():
	# read and set variables for s3 storage
	env_items = readEnv(filename="env")
	env_items["BUCKET_NAME"] = "dataset" # S3 bucket name
	env_items["PREFIX"] = "db_prefix/db_dataset"  # destination path
	env_items["LOCAL_DIRECTORY"] = "./test" # source directory
	env_items["NUM_CPUS"] = multiprocessing.cpu_count() - 1

	# make a filename list
	files = makeFilelist(env_items["LOCAL_DIRECTORY"])
	if len(files) == 0:
		exit(0)

	# upload files
	s3_storage = S3Storage(env_items)
	s3_storage.upload_files(files)


if __name__ == "__main__":
	main()