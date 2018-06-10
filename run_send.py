# coding=utf-8
import pandas as pd
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import os
from email.mime.text import MIMEText
import smtplib
import sys
import json
import csv
from collections import defaultdict

class info_sender():
    def __init__(self, user_config_file, rent_info_file):
        self.user_config_file = user_config_file
        self.rent_info_file = rent_info_file
        self.user_config_dict = defaultdict(dict)
        self.user_sent_list = defaultdict(list)
        self.rent_df = None
        self.user_sendbuff = {}

        self.sent_list_file = "user_sent_list.json"

    def load_config(self):
        print("Loading User Config.")
        user_config_df = pd.read_excel(self.user_config_file)
        for idx, row in user_config_df.iterrows():
            email = row["邮箱"]
            if pd.isnull(email): continue
            keywords = [] if pd.isnull(row["关键词"]) else row["关键词"].split(",")
            stopwords = [] if pd.isnull(row["停用词"]) else row["停用词"].split(",")
            max_rent = None if pd.isnull(row["最大租金"]) else row["最大租金"]
            self.user_config_dict[email]["keywords"] = keywords
            self.user_config_dict[email]["stopwords"] = stopwords
            self.user_config_dict[email]["max_rent"] = max_rent

    def load_rent_info(self):
        print("Loading Rent Info.")
        self.rent_df = pd.read_csv("rent_info.tsv", error_bad_lines=False, quoting=csv.QUOTE_NONE,
                                    sep="\t", names=["title", "rent", "time", "url", "content"])
        self.rent_df.dropna(inplace=True)

    def load_sent_list(self):
        try:
            print("Loading Sent List.")
            if os.path.isfile(self.sent_list_file):
                with open(self.sent_list_file) as fr:
                    self.user_sent_list = json.load(fr)
        except Exception as msg:
            print("Error: [{}]".format(msg))
            print("Initialize new.")

    def filter_info(self, info, user_email, config_info):
        if info["url"] in self.user_sent_list[user_email]: 
            return False 

        if config_info["stopwords"]:
            for stop_word in config_info["stopwords"]:
                if stop_word in info["title"] or stop_word in info["content"]:
                    return False
        
        if config_info["max_rent"]:
            if info["rent"] != "自己看" and int(info["rent"]) > config_info["max_rent"]:
                return False

        if config_info["keywords"]:
            for key_word in config_info["keywords"]:
                if key_word in info["title"] or key_word in info["content"]:
                    print("命中", key_word)
                    if info["rent"] == "自己看":
                        return True
                    elif config_info["max_rent"] and int(info["rent"]) < config_info["max_rent"]:
                        return True
        return False

    def filter_rent_info(self, user_email, config_info):
        send_list = []
        for idx, info in self.rent_df.iterrows():
            if self.filter_info(info, user_email, config_info):
                send_list.append(info)
        return send_list

    def filter_for_users(self):
        for user_email, config_info in self.user_config_dict.items():
            print("Sending to [{}]".format(user_email))
            send_list = self.filter_rent_info(user_email, config_info)
            format_msg = ""
            for idx, info in enumerate(send_list):
                self.user_sent_list[user_email].append(info["url"])
                format_msg += "*--------------*\n"
                format_msg += "标题:[{}]\n".format(info["title"])
                format_msg += "租金:[{}]\n".format(info["rent"].replace("自己看", "NULL"))
                format_msg += "地址:[{}]\n".format(info["url"])
                format_msg += "*--------------*\n"
                format_msg += "\n\n"
            self.user_sendbuff[user_email] = format_msg
        
        print("Dumping Sent List.")
        with open(self.sent_list_file, "w") as fw:
            json.dump(self.user_sent_list, fw, indent=4)

    def send_email(self):
        uptime = "_".join(time.asctime( time.localtime(time.time()) ).split()[1:-1]).replace(":", "_")[0:-3]
        from_addr = "346296203@qq.com"
        with open("passward.txt") as fr:
            password = fr.readline().strip()
        server = smtplib.SMTP_SSL(host='smtp.qq.com', port=465)
        server.login(from_addr, password)
        for to_addr, format_msg in self.user_sendbuff.items():
            if format_msg:
                declare = \
                "=============\n" + \
                "1. 每5小时会发送发送时间前5小时在豆瓣【天河，荔湾，越秀】租房小组的过滤后的新增帖。\n" + \
                "2. 第一封邮件内容可能会较多，包含近期历史帖子。\n" + \
                "3. 目前价格识别算法还不完善，有些需要点进去帖子里面看价格。\n" + \
                "=============\n"
                format_msg = declare + format_msg
                msg = MIMEText(format_msg)
                msg["Subject"] = "租房信息【{}】".format(uptime)
                msg["From"]    = from_addr
                msg["To"]      = to_addr
                server.sendmail(from_addr, to_addr, msg.as_string())
                time.sleep(3)
        server.quit()

    def send(self):
            print("Start Sending.")
            self.load_config()
            self.load_rent_info()
            self.load_sent_list()
            self.filter_for_users()
            self.send_email()
            print("Finish Sending.")

if __name__ == "__main__":
    send_interval = sys.argv[1]

    user_config_file = "user_config.xls"
    rent_info_file = "rent_info.tsv"
    sender = info_sender(user_config_file, rent_info_file)
    sched = BlockingScheduler(timezone="Asia/Shanghai")

    # 每指定频率发送一次
    sched.add_job(sender.send, 'interval', seconds=int(send_interval))
    sched.start()


