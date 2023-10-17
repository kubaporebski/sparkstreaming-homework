import time
import os
import sys
import re
import subprocess

# configuration class 
class Config:
    # seconds, float value
    WAIT_BETWEEN_FILES = float(os.getenv("WAIT_TIME", "12.34"))

    # GCP bucket name 
    BUCKET_NAME = os.getenv("BUCKET_NAME", "storage-bucket-select-gar")


def main(argv):
    dir = os.curdir
    if len(argv) > 1:
        dir = argv[1]

    if not os.path.isdir(f"{dir}/hotel-weather"):
        print("This directory doesn't contain `hotel-weather` directory")
        exit()

    
    # set containing paths - directories' names (where parquet files are located), grouped by a day
    parquets = set()

    # process files recursively 
    print(f"Collecting data from directory [{dir}]... ")
    for (dirpath, dirnames, filenames) in os.walk(dir):
        part = re.match(r".*(year=[0-9]{4}.month=[0-9]{2}.day=[0-9]{2}).*", dirpath)
        if part is None:
            continue
        grp = part.group(1)
        parquets.add(grp)

    print(f"Sending files is about to begin... ")
    # here we have our files list, grouped by a day 
    for days in parquets:
        print(days)

        # we will use `gsutil` tool
        # usage like: gsutil -m cp -r "year=2017\month=08\day=10" "gs://storage-bucket-select-gar/m13sparkstreaming/hotel-weather/year=2017/month=08/day=10"
        
        local_days_path = f"{dir}/hotel-weather/{days}"
        remote_days_path = f"gs://{Config.BUCKET_NAME}/m13sparkstreaming/hotel-weather/{days}"
        gsutil_cmd = ["gsutil", "-m", "cp", "-r", local_days_path, remote_days_path]
        print(f"Sending directory [{local_days_path}] using `{' '.join(gsutil_cmd)}`... You can stop anytime by pressing CTRL-C.")

        subprocess.check_output(gsutil_cmd, shell=True)

        # sleep some time before sending batch of next day's files
        print(f"Wait {Config.WAIT_BETWEEN_FILES} seconds...")
        time.sleep(Config.WAIT_BETWEEN_FILES)


if __name__ == "__main__":
    main(sys.argv)



