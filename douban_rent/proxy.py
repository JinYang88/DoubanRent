import random
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
from scrapy import log
import re
import base64

# 代理服务器
proxyServer = "http://http-dyn.abuyun.com:9020"

# 代理隧道验证信息
proxyUser = "H21L8A02A50X8TSD"
proxyPass = "7FC9A765E9D70BEB"

# for Python2
# proxyAuth = "Basic " + base64.b64encode(proxyUser + ":" + proxyPass)

# for Python3
proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")

class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta["proxy"] = proxyServer
        request.headers["Proxy-Authorization"] = proxyAuth     