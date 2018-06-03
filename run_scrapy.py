import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
import sys

def craw_data():
    print("Start crawing data..")
    subprocess.call("python entry_point.py", shell=True)

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="Asia/Shanghai")
    sched.add_job(craw_data, 'interval', seconds=int(sys.argv[1]))
    sched.start()
