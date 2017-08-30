# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):

    name = scrapy.Field() #方剂名
    href = scrapy.Field()
    prescription = scrapy.Field() #处方中的中药
    prescriptionText = scrapy.Field()#处方
    symptoms = scrapy.Field()#网站中带链接的症状名词
    symptomText = scrapy.Field()#功能主治
    usage = scrapy.Field()#用法用量
    reference = scrapy.Field()#引用文献