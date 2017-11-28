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

	column_fields={1: 'party_code', 
	2:'party_name', 
	3:'party_founding_date', 	
	5:'party_id'}
	def parse(self, response): 
		# tds = response.xpath('//tbody/tr//td[@class="tabelas"]')
		tds = response.xpath('//tbody/tr//td')
		ncols=6
		parties= {} 

		for i, td in enumerate(tds):			
			row = int(i / ncols)
			col = i - row*ncols
			
			if col in self.column_fields: 
				values=  td.xpath('.//text()').extract()

				# Due to weird formatting we must grab the first non-empty value
				values=list(filter(lambda x : not(x in stopwords), values))
				field_name=self.column_fields[col]
				parties[field_name]= values[0] if len(values)>0 else None			
			
			if (col == ncols-1):
				yield parties
				parties={}


