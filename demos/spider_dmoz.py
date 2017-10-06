# -*- coding: utf-8 -*-
#scrapy runspider spider_dmoz.py
import scrapy

class DmozItem(scrapy.Item):
   title = scrapy.Field()
   link = scrapy.Field()
   desc = scrapy.Field()


class DmozSpider(scrapy.Spider):
	name = "dmoz"
	allowed_domains = ["dmoz.org"]
	start_urls = [
		"http://www.dmoz.org/Computers/Programming/Languages/Python/Books/",
		"http://www.dmoz.org/Computers/Programming/Languages/Python/Resources/"
	]

	def parse(self, response):
		
		for sel in response.xpath('//ul/li'):
			title = sel.xpath('a/text()').extract()
			link = sel.xpath('a/@href').extract()
			desc = sel.xpath('text()').extract()
      
			print(title, link, desc)