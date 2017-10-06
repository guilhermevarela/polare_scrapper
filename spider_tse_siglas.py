# -*- coding: utf-8 -*-
'''
	Date: Oct 16th, 2017

	Author: Guilherme Varela

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse'
'''
import scrapy

class TseSiglasSpider(scrapy.Spider):
	name= 'tsesiglas'
	start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse']

	def parse(self, response): 
		first= True
		headers= {} 
		for title in response.css('table.grid.listing tbody'):			
			yield {'title': title.css('::text').extract_first()}

