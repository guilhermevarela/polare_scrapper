# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla'

	Scrapy running: scrapy runspider spider_tse_camara.py

	Scrapy run + store: scrapy runspider  spider_open_data_camara.py -o camara_open_data_parties.json
'''
import scrapy
import json 

# https://dadosabertos.camara.leg.br/swagger/api.html
#https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla
URL_OPEN_DATA_CAMARA= 'https://dadosabertos.camara.leg.br/api/v2/'

class RequestHeaderItem(scrapy.Item): 
	partidoid = scrapy.Field() 	
	sigla=      scrapy.Field() 
	nome=       scrapy.Field() 
	uri=        scrapy.Field() 



class CamaraOpenDataSpider(scrapy.Spider):
	name= 'camara_open_data'
	# start_urls = ['https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla']

	def start_requests(self): 
		'''
			Refreshes all partido ids
								and than scrapes results
		'''
		url = URL_OPEN_DATA_CAMARA
		url += 'partidos?itens=99999&ordenarPor=sigla'
		req = scrapy.Request(url, 
			self.parse_partylist, 
			headers= {'accept': 'application/json'}
		)
		yield req

	def parse_partylist(self, response): 
		# import code; code.interact(local=dict(globals(), **locals()))
		response_json = json.loads(response.body_as_unicode())
		yield dict([('header', response_json['dados'])]) # saves all partylists
		self.partylist = response_json['dados']		
		self.parties= {} 
		for party in self.partylist: 
			req = scrapy.Request(party['uri'], 
				self.parse_party, 
				headers= {'accept': 'application/json'}, 				
			)
			yield req 
		# yield self.parties

	def parse_party(self, response):
	 		response_json = json.loads(response.body_as_unicode())
	 		sigla= response_json['dados']['sigla']
	 		# self.parties[sigla]= response_json['dados']  
	 		yield dict([(sigla,response_json['dados'])])

	# def parse(self, response): 	
	# 	tds = response.xpath('//tbody/tr//td')
	# 	parties= {} 
	# 	for td in tds:			
	# 		value=  td.xpath('.//text()').extract_first()
	# 		link=   td.xpath('.//a/@href').extract_first()
			
	# 		if link: 
	# 			yield scrapy.Request(url=link, callback=self.parse_party) 				

	# def parse_party(self, response): 
	# 	first= True
	# 	info= {} 
	# 	info['title']=  response.xpath('.//div[@id="tituloInterno"]/h2/text()').extract_first()
		
	# 	contents= response.xpath('.//div[@id="textoConteudo"]//p') # prevents <strong></strong> tag --> len 2 array 
	# 	# variations for contents[i].xpath('.//text()').extract()
	# 	# ['Nome: Partido do Movimento Democrático Brasileiro']
	# 	# ['Sigla: ', 'PMDB']		
	# 	for content in contents:
	# 		#Filter 	
	# 		arr = content.xpath('.//text()').extract()
	# 		info = tokenizer(arr, info)
	# 		print(content)

	# 	yield dict([(info['sigla'], info)])
	# 