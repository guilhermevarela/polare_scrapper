# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla'

	Scrapy running: scrapy runspider spider_politicalparties.py

	Scrapy run + store: scrapy runspider  spider_political_parties.py -o political_parties.json
'''
import scrapy
import json 

#https://dadosabertos.camara.leg.br/swagger/api.html
URL_OPEN_DATA_CAMARA_API_V2= 'https://dadosabertos.camara.leg.br/api/v2/'



class PoliticalPartyMembershipSpider(scrapy.Spider):
	name= 'political_party_membership'
	# start_urls = ['https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla']

	def start_requests(self): 
		'''
			Refreshes all partido ids and than scrapes results
		'''
		url = URL_OPEN_DATA_CAMARA_API_V2
		url += 'partidos?itens=99999&ordenarPor=sigla'
		req = scrapy.Request(url, 
			self.parse_partylist, 
			headers= {'accept': 'application/json'}
		)
		yield req

	def parse_partylist(self, response): 
		response_json = json.loads(response.body_as_unicode())		
		self.partylist = response_json['dados']		
		self.parties= {} 
		for party in self.partylist: 
			req = scrapy.Request(party['uri'], 
				self.parse_party, 
				headers= {'accept': 'application/json'}, 				
			)
			yield req 

	def parse_party(self, response):
	 		response_json = json.loads(response.body_as_unicode())
	 		sigla= response_json['dados']['sigla']
	 		yield response_json['dados']

