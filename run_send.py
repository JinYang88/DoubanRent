# coding=utf-8
import pandas as pd
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import os
from email.mime.text import MIMEText
import smtplib
import sys

keywords = [
"1房1厅", "1室1厅", "一房一厅", "一室一厅", "宠物",
"广州火车站", "小北", "越秀公园", "纪念堂", 
"公园前", "海珠广场", "北京路", "团一大广场",
"杨箕", "五羊邨", "员村", "猎德", "潭村",
"烈士陵园", "东山口", "东湖", "动物园",
"区庄", "淘金", "黄花岗"
]

stopwords = ["合租", "次卧", "主卧", "限男生", "三房", "两房", "二房", "求", "3房", "2房"]
maxrent = 3000

def send_email(to_addrs, content):
    
    uptime = "_".join(time.asctime( time.localtime(time.time()) ).split()[1:-1]).replace(":", "_")[0:-3]

    from_addr = "346296203@qq.com"
    with open("passward.txt") as fr:
        password = fr.readline().strip()
    server = smtplib.SMTP_SSL(host='smtp.qq.com', port=465)
    server.login(from_addr, password)

    for to_addr in to_addrs:
        msg = MIMEText(content)
        msg["Subject"] = "看看看看看有啥好房纸！【{}】".format(uptime)
        msg["From"]    = from_addr
        msg["To"]      = to_addr
        server.sendmail(from_addr, to_addr, msg.as_string())
    server.quit()

def filter(row, sent_list):
    # not in sent
    if row["url"] in sent_list: 
        return False 
    for stop_word in stopwords:
        if stop_word in row["title"] or stop_word in row["content"]:
            return False
    if row["rent"] != "自己看" and int(row["rent"]) > maxrent:
        return False
    for key_word in keywords:
        if key_word in row["title"] or key_word in row["content"]:
            if row["rent"] == "自己看":
                return True
            elif int(row["rent"]) < maxrent:
                return True
    return False

def send_info(to_addrs):
    print("Sending info...")
    try:
        fr = open("sent_list.txt", 'r')
        sent_list = [line.strip() for line in fr]
        fr.close()
    except:
        sent_list = []
    rent_df = pd.read_csv("rent_info.tsv", sep="\t", names=["title", "rent", "time", "url", "content"])
    rent_df.dropna(inplace=True)

    send_buffer = []
    fw = open("sent_list.txt", "a")
    for idx, row in rent_df.iterrows():
        if filter(row, sent_list):
            sent_item = "---\n{}\n租金:{}\n{}\n".format(row['title'], row['rent'], row["url"])
            fw.write(row['url'] + "\n")
            send_buffer.append(sent_item)
    fw.close()
    
    idx = 0
    msg = ""
    while idx < len(send_buffer):
        msg += "\n".join(send_buffer[idx: idx+5])
        msg += "\n------------\n"
        idx += 5
    if msg:
        send_email(to_addrs, msg)
    print("Finish sending.")


if __name__ == "__main__":
    send_interval = sys.argv[1]

    # send_email("346296203@qq.com", "测试")
    # to_addrs = ["346296203@qq.com", "543693275@qq.com"]
    to_addrs = ["346296203@qq.com"]
    # send_info(to_addrs)

    sched = BlockingScheduler(timezone="Asia/Shanghai")
    # 每2小时抓一次数据
    # sched.add_job(craw_data, 'interval', seconds=7200)
    # 每指定频率发送一次
    sched.add_job(send_info, 'interval', seconds=int(send_interval), args=[to_addrs])
    sched.start()


