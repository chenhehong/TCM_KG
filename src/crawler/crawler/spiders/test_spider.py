# -*- coding: utf-8 -*-


import scrapy
from scrapy.selector import Selector
import re

class TestSpider(scrapy.spiders.Spider):

    name = "test"
    allowed_domains = ["zhongyaocai360.com"]
    start_urls = [
        "http://zhongyaocai360.com/l/liuyelongdan.html"
    ]

    def parse(self, response):
        nameHtml = response.xpath('//head/title/text()').extract()[0]
        name = nameHtml.split('_')[0]
        href = response.url
        # print(name+"|"+href)
        refs = response.xpath('//div[@class="spider"]/dl[1]/dd')
        #计算文献记录的数量
        num = 0
        for ref in refs:
            num+=1
            smell = response.xpath(u'//div[@class="spider"]/p[contains(.,"【性味】")]/text()').extract()
            smellLen = len(smell)
            if(num>smellLen):
                smell = " "
            else:
                smell = smell[num-1]
                smell = re.sub(ur'【性味】',"",smell)

            organ = response.xpath(u'//div[@class="spider"]/p[contains(.,"【归经】")]/text()').extract()
            organLen = len(organ)
            if(num>organLen):
                organ = " "
            else:
                organ = organ[num-1]
                organ = re.sub(ur'【归经】',"",organ)

            symptomTextHtml = response.xpath('//div[@class="spider"]/div[@class="gnzzp"]/p[@class="zz"]')[num-1]
            symptomText = symptomTextHtml.extract()
            symptomText = re.sub(r'<[^>]*>',"",symptomText)
            symptoms = re.sub(ur'【功能主治】',"",symptomText)#功能主治

            referenceHtml = response.xpath(u'//div[@class="spider"]/p[contains(.,"【摘录】")]/a[1]/text()').extract()
            referenceLen = len(referenceHtml)
            if(num>referenceLen):
                reference = " "
            else:
                referenceHtml = referenceHtml[num-1]
                reference = re.sub(ur'【摘录】',"",referenceHtml)#摘录
            split = "||"
            str = name+split+href+split+smell+split+organ+split+symptoms+split+reference
            print str
