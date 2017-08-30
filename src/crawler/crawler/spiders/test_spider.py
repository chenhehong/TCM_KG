# -*- coding: utf-8 -*-


import scrapy
from scrapy.selector import Selector
import re

class TestSpider(scrapy.spiders.Spider):

    name = "test"
    allowed_domains = ["zhongyaofangji.com"]
    start_urls = [
        "http://zhongyaofangji.com/a/anqisan.html"
    ]

    def parse(self, response):
        nameHtml = response.xpath('//head/title/text()').extract()[0]
        name = nameHtml.split('_')[0]
        href = response.url
        refs = response.xpath('//div[@class="spider"]/dl[1]/dd')
        #计算文献记录的数量
        num = 0
        for ref in refs:
            num+=1
            prescriptionHtml = response.xpath(u'//div[@class="spider"]/p[contains(.,"【处方】")]')[num-1]
            prescription=prescriptionHtml.extract()
            prescription = re.sub(r'<[^>]*>',"",prescription)
            prescription = re.sub(ur'【处方】',"",prescription)#处方
            prescriptionTextHtml = prescriptionHtml.xpath('a/text()').extract()
            prescriptionText = " " # 中药实体
            flag = False
            for p in prescriptionTextHtml:
                if(flag==True):
                    prescriptionText = prescriptionText+','+ p
                else:
                    prescriptionText = prescriptionText+p
                    flag = True
            symptomTextHtml = response.xpath('//div[@class="spider"]/div[@class="yfpzz"]/p[1]')[num-1]
            symptomText = symptomTextHtml.extract()
            symptomText = re.sub(r'<[^>]*>',"",symptomText)
            symptomText = re.sub(ur'【功能主治】',"",symptomText)#功能主治
            symptomsHtml = symptomTextHtml.xpath(u'a[contains(@title,"方剂主治")]/text()').extract()
            symptoms = " " # 网站中带链接的症状名词
            flag = False
            for s in symptomsHtml:
                if (flag == True):
                    symptoms = symptoms + ',' + s
                else:
                    symptoms = symptoms + s
                    flag = True
            usageHtml = response.xpath(u'//div[@class="spider"]/p[contains(.,"【用法用量】")]')[num-1]
            usage=usageHtml.extract()
            usage = re.sub(r'<[^>]*>',"",usage)
            usage = re.sub(ur'【用法用量】',"",usage)#用法用量
            referenceHtml = response.xpath(u'//div[@class="spider"]/p[contains(.,"【摘录】")]')[num-1]
            reference=referenceHtml.extract()
            reference = re.sub(r'<[^>]*>',"",reference)
            reference = re.sub(ur'【摘录】',"",reference)#摘录
            split = "||"
            str = name+split+href+split+prescription+split+prescriptionText+split+symptomText+split+symptoms+split+usage+split+reference
            print str
