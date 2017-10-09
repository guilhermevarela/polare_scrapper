# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse'

	Scrapy running: scrapy runspider spider_tse_parties.py

	Scrapy run + store: scrapy runspider  spider_tse_parties.py -o tse_parties.json
'''
import scrapy

class TsePartiesSpider(scrapy.Spider):
	name= 'tseparties'
	start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse']
	
	def parse(self, response): 
		first= True
		trs = response.xpath('//tbody/tr')
		ths = trs.xpath('./th')
		headers = [] 
		for th in ths:
			h = th.xpath('.//text()').extract_first()
			headers.append(h)
			
		yield {'headers': headers}
		

		tds = trs.xpath('.//td')
		siglas= []
		links=  []  
		for i, td in enumerate(tds):			
			n = i % len(headers)
			if i>0 and n == 0:
				yield {k:v for k,v in zip(headers, siglas)}				
				siglas= [] 
			
			value = td.xpath('.//text()').extract_first()
			siglas.append(value)			
		yield {k:v for k,v in zip(headers, siglas)}				

