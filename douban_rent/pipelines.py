# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class DoubanRentPipeline(object):
    def __init__(self):
        self.file = open('rent_info.tsv', 'a', encoding='utf-8')
    def process_item(self, item, spider):
        # print("Write item into file done.")
        # print(item)
        self.file.write("{}\t{}\t{}\t{}\t{}\n"
                        .format(
                        item["title"],
                        item["rent"],
                        item["posttime"],
                        item["url"],
                        item["content"]
                        ))
        return item
    def spider_closed(self, spider):
        self.file.close()

