# -*- coding: utf-8 -*-
'''
	Date: Oct 06th, 2017

	Author: Guilherme Varela

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse'
	scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro'
	Scrapy running: scrapy runspider spider_tse_siglas.py

	Scrapy run + store: scrapy runspider  spider_tse_siglas.py -o siglas.json
'''
import scrapy

class TseSiglasSpider(scrapy.Spider):
	name= 'tsesiglas'
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
		links= [] 
		for i, td in enumerate(tds):			
			n = i % len(headers)
			if i>0 and n == 0:
				yield { 
					'texts': {k:v for k,v in zip(headers, siglas)},
					'links': {k:v for k,v in zip(headers, links) if v},
				}								
				siglas= [] 
				links= [] 
			
			value = td.xpath('.//text()').extract_first()
			link=   td.xpath('.//a/@href').extract_first()
			siglas.append(value)			
			links.append(link)			
		yield { 
		'texts': {k:v for k,v in zip(headers, siglas)},
		'links': {k:v for k,v in zip(headers, links) if v},
		}								

