import pandas as pd
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import itchat
import os
import subprocess

keywords = ["一房一厅", "一室一厅", "宠物"]

def filter(row, sent_list):
    # not in sent
    if row["url"] in sent_list: 
        return False 
    if row["rent"] != "自己看" and int(row["rent"]) < 2000:
        return True
    # keywords in title or content
    for key_word in keywords:
        if key_word in row["title"] or key_word in row["content"]:
            return True
    return False

def send_info():
    print("Sending info...")
    fr = open("sent_list.txt", 'r')
    sent_list = [line.strip() for line in fr]
    fr.close()
    rent_df = pd.read_csv("rent_info.tsv", sep="\t", names=["title", "rent", "time", "url", "content"])

    send_buffer = []
    fw = open("sent_list.txt", "a")
    for idx, row in rent_df.iterrows():
        if filter(row, sent_list):
            sent_item = "---\n{}\n租金:{}\n{}\n---".format(row['title'], row['rent'], row["url"])
            fw.write(row['url'] + "\n")
            # print(sent_item)
            send_buffer.append(sent_item)
            time.sleep(2)
    idx = 0
    while idx < len(send_buffer):
        sent_msgs = send_buffer[idx: idx+5]
        idx += 5
        msg = "\n".join(sent_msgs)
        itchat.send_msg(msg=msg, toUserName="filehelper")
        time.sleep(2)

    fw.close()
    print("Finish sending.")

def craw_data():
    subprocess.call("python entry_point.py", shell=True)

def job2():
    print("222")

if __name__ == "__main__":
    itchat.auto_login(enableCmdQR=2, hotReload=True)
    sched = BlockingScheduler()
    # 每1小时抓一次数据
    sched.add_job(send_info, 'interval', seconds=5)
    # 每3小时发送一次
    # sched.add_job(job2, 'interval', seconds=5)
    sched.start()
    # my_job()


