#coding = utf-8

import os
import scrapy
import urllib.parse
from scrapy.http import Request
import re
from bs4 import BeautifulSoup
import sys
from lxml import etree
import json
import scrapy
import urllib.parse
from scrapy.http import Request
import re
import pandas as pd
import datetime
from lxml import etree
import time
import requests
from bs4 import BeautifulSoup
import csv

class DoubanRent(scrapy.Spider):
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
      "host" : "http://http-dyn.abuyun.com",
      "port" : 9020,
      "user" : "H21L8A02A50X8TSD",
      "pass" : "7FC9A765E9D70BEB",
    }
    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }

    name = 'DoubanRent'
    last_hour = 10
    headers = {
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }
    mintime = (datetime.datetime.now() + datetime.timedelta(hours=-last_hour)).strftime('%m-%d %H:%M')
    # mintime = "06-02 14:00"
    if not os.path.isfile("rent_info.tsv"):
        open("rent_info.tsv","w+").close()
    save_urls = pd.read_csv("rent_info.tsv", sep="\t", encoding="utf-8", error_bad_lines=False, quoting=csv.QUOTE_NONE,
                             names=["Title","PostTime","Url","Content"])["Url"]



    def start_requests(self):
        print(self.mintime)
        start_urls = [
            # 深圳租房团小组
            "https://www.douban.com/group/106955/discussion?",
            "https://www.douban.com/group/nanshanzufang/discussion?",
            "https://www.douban.com/group/futianzufang/discussion?",
            "https://www.douban.com/group/baoanzufang/discussion?",
            "https://www.douban.com/group/longhuazufang/discussion?",
            "https://www.douban.com/group/luohuzufang/discussion?",
            "https://www.douban.com/group/yantianzufang/discussion?",
            "https://www.douban.com/group/longgangzufang/discussion?"
        ]
        for url in start_urls:
            for pagenum in range(40):
                nexturl = url + "start={}".format(pagenum*25)
                tmp_response = requests.get(url=nexturl, headers=self.headers)
                selector = etree.HTML(tmp_response.text)
                lasttime = selector.xpath("//*[@class='time']/text()")[-1]
                time.sleep(2)
                if lasttime < self.mintime: break
                yield Request(url=nexturl, callback=self.parse_posts, headers=self.headers)
                
    def parse_posts(self, response):
        hrefs = response.xpath("//*[@class='olt']//*[@class='title']/a/@href").extract()
        times = response.xpath("//*[@class='time']/text()").extract()
        if times[-1] < self.mintime: return
        for idx, url in enumerate(hrefs[:-1]):
            if url not in self.save_urls.tolist():
                yield Request(url=url, callback=self.parse_details, headers=self.headers)
        
    def parse_details(self, response):
        url = response.url
        title = response.xpath("//*[@id='content']/h1/text()").extract()[0].strip()
        content = response.xpath("//*[@class='topic-richtext']/p/text()").extract()
        content = "|---|".join(content)
        content = re.sub(r"[\n\r]", "|---|", content)
        posttime = response.xpath("//*[@class='color-green']/text()").extract()[0]
        try:
            try:
                rent = re.findall(r".{0,5}\d{4}.{5}", content)
                rent = list(filter(lambda x: re.findall("[预算均转元租押]", x), rent))
                rent = re.findall("\d{4}", rent[0])[0]
            except:
                rent = re.findall(r".{0,5}\d{4}.{5}", title)
                rent = list(filter(lambda x: re.findall("[预算均转元租押]", x), rent))
                rent = re.findall("\d{4}", rent[0])[0]
        except:
            rent = "自己看"
        item = {}
        item["rent"] = rent
        item["title"] = title
        item["posttime"] = posttime
        item["url"] = url
        item["content"] = content
        yield item
