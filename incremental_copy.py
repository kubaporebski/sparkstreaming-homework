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
        raise "Not right path"
    
    # dictionary, where:
    #   keys are relative paths of parquet files, grouped by day,
    #   values are list of parquet files in a given day
    parquets = {}

    # process files recursively 
    print(f"Starting incremental copy of data from directory [{dir}]... ")
    for (dirpath, dirnames, filenames) in os.walk(dir):
        part = re.match(r".*(year=[0-9]{4}.month=[0-9]{2}.day=[0-9]{2}).*", dirpath)
        if part is None:
            continue
        grp = part.group(1)

        for filename in filenames:
            if not filename.endswith(".parquet"):
                continue

            fn = re.sub(r".*hotel\-weather.", "", f"{dirpath}/{filename}")
            parquets.setdefault(grp, []).append(fn)


    # here we have our files list, grouped by a day 
    for days in parquets:
        print(days)

        # we will use `gsutil` tool
        # usage like: gsutil -m cp -r "year=2017\month=08\day=10" "gs://storage-bucket-select-gar/m13sparkstreaming/hotel-weather/year=2017/month=08/day=10"
        
        full_sent_dir_path = f"{dir}/hotel-weather/{days}"
        gsutil_cmd = ["gsutil", "-m", "cp", "-r", full_sent_dir_path, f"gs://{Config.BUCKET_NAME}/m13sparkstreaming/hotel-weather/{days}"]
        print(f"Sending directory [{full_sent_dir_path}] using {gsutil_cmd}... ")

        subprocess.check_output(gsutil_cmd, shell=True)

        # sleep some time before sending batch of next day's files
        print(f"Wait {Config.WAIT_BETWEEN_FILES} seconds...")
        time.sleep(Config.WAIT_BETWEEN_FILES)


if __name__ == "__main__":
    main(sys.argv)



