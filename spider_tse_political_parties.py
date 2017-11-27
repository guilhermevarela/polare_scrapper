# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro'

	Scrapy running: scrapy runspider spider_tse_parties.py

	Scrapy run + store: scrapy runspider spider_tse_political_parties.py -o tse_political_parties.json
'''
import scrapy


stopwords=[
	'\n'
]
class TsePoliticalPartiesSpider(scrapy.Spider):
	name= 'tse_political_parties'
	start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse']
	# start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro']
	
	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	keys=['party_code', 'party_name', 'party_founding_date', 'party_president', 'party_founding_number']
	def parse(self, response): 
		# tds = response.xpath('//tbody/tr//td[@class="tabelas"]')
		tds = response.xpath('//tbody/tr//td')
		parties= {} 
		count=0

		for i, td in enumerate(tds):			
			if not(count==3): 
				values=  td.xpath('.//text()').extract()

				# print('%d:%s' % (i, value))
				values=list(filter(lambda x : not(x in stopwords), values))
				parties[self.keys[count]]= values[0] if len(values)>0 else None			
				if (count == 4):
					yield parties
					parties={}
					count=-1
			count+=1 
