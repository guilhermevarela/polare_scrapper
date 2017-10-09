# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro'

	Scrapy running: scrapy runspider spider_tse_parties.py

	Scrapy run + store: scrapy runspider  spider_tse_parties.py -o tse_parties.json
'''
import scrapy

def tokenizer(arr0, f2v={}):
	'''
		Converts into hashes lowercasing keys
	'''
	# import code; code.interact(local=dict(globals(), **locals()))
	splitter= lambda x: x.split(':')		
	arr= map(splitter, arr0)

	#flatten array & strip elements
	arr= [e.strip() for a in arr for e in a] 

	#filter empty values 
	arr= list(filter(lambda x: not(x==''), arr))

	#lower case field names 
	keys= [key.lower() for ind, key in enumerate(arr) if ind % 2==0]
	values= [val for ind, val in enumerate(arr) if (ind % 2)]
	
	f2v.update({k:v for k,v in zip(keys, values)} ) 
	return f2v


class TsePartiesSpider(scrapy.Spider):
	name= 'tseparties'
	start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse']
	# start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro']
	
	def parse(self, response): 
		tds = response.xpath('//tbody/tr//td')
		parties= {} 
		for td in tds:			
			value=  td.xpath('.//text()').extract_first()
			link=   td.xpath('.//a/@href').extract_first()
			
			if link: 
				yield scrapy.Request(url=link, callback=self.parse_party) 				

	def parse_party(self, response): 
		first= True
		info= {} 
		info['title']=  response.xpath('.//div[@id="tituloInterno"]/h2/text()').extract_first()
		
		contents= response.xpath('.//div[@id="textoConteudo"]//p') # prevents <strong></strong> tag --> len 2 array 
		# variations for contents[i].xpath('.//text()').extract()
		# ['Nome: Partido do Movimento Democrático Brasileiro']
		# ['Sigla: ', 'PMDB']		
		for content in contents:
			#Filter 	
			arr = content.xpath('.//text()').extract()
			info = tokenizer(arr, info)
			print(content)

		yield dict([(info['sigla'], info)])
	