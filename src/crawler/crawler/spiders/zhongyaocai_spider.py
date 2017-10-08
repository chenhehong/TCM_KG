# -*- coding: utf-8 -*-


import scrapy
from scrapy.selector import Selector
import re

class ZhongyaocaiSpider(scrapy.spiders.Spider):

    name = "zhongyaocai"
    allowed_domains = ["zhongyaocai360.com"]
    start_urls = [
        "http://zhongyaocai360.com/A/",
        "http://zhongyaocai360.com/B/",
        "http://zhongyaocai360.com/C/",
        "http://zhongyaocai360.com/D/",
        "http://zhongyaocai360.com/E/",
        "http://zhongyaocai360.com/F/",
        "http://zhongyaocai360.com/G/",
        "http://zhongyaocai360.com/H/",
        "http://zhongyaocai360.com/I/",
        "http://zhongyaocai360.com/J/",
        "http://zhongyaocai360.com/K/",
        "http://zhongyaocai360.com/L/",
        "http://zhongyaocai360.com/M/",
        "http://zhongyaocai360.com/N/",
        "http://zhongyaocai360.com/O/",
        "http://zhongyaocai360.com/P/",
        "http://zhongyaocai360.com/Q/",
        "http://zhongyaocai360.com/R/",
        "http://zhongyaocai360.com/S/",
        "http://zhongyaocai360.com/T/",
        "http://zhongyaocai360.com/U/",
        "http://zhongyaocai360.com/V/",
        "http://zhongyaocai360.com/W/",
        "http://zhongyaocai360.com/X/",
        "http://zhongyaocai360.com/Y/",
        "http://zhongyaocai360.com/Z/"
    ]

    def parse(self, response):
        self.log("begins  % s" % response.url)
        selector = Selector(response)
        fangJis = selector.xpath('//ul[@class="uzyc"][1]/li')
        for fangji in fangJis:
            href = fangji.xpath('a/@href').extract()[0]
            name = fangji.xpath('a/text()').extract()[0]
            self.log("%s url: % s" % (name,href))
            with open("url.txt", 'a') as f:
                f.write((name+"||"+href+'\n').encode('utf-8'))
            yield scrapy.Request(url=href,callback=self.parse_content)

    def parse_content(self, response):
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
            # print str
            with open("data.txt", 'a') as f:
                f.write((str + '\n').encode('utf-8'))

