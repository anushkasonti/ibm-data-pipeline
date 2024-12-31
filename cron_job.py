import schedule
import time
from datetime import datetime
import subprocess

def job():
    print("Job started at", datetime.now())
    subprocess.run(["C:\Program Files\Python311\python.exe", "grm_project.py"])

# Schedule the job to run today (2/5/2024) at 1:50 PM
scheduled_time = datetime(2024, 2, 5, 23, 18)
schedule.every().day.at(scheduled_time.strftime("%H:%M")).do(job)

while schedule.get_jobs():
    schedule.run_pending()
    time.sleep(1)
