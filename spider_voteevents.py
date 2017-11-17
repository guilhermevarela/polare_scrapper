# -*- coding: utf-8 -*-
'''
	Date: Nov 14th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	fetch all deputies's votes for all vote events for a given proposition

	proposition:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/1215809'

	voteevents by preps:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes" -H  "accept: application/json"	
	voteevent details: (*INTERESTING: PARTY AFFILIATIONS*)
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/votacoes/6517'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/votacoes/6517" -H  "accept: application/json"
	votes:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/votacoes/6516/votos?itens=513'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/votacoes/6516/votos?itens=513" -H  "accept: application/xml"


	Scrapy running: scrapy runspider spider_voteevents.py

	Scrapy run + store: scrapy runspider  spider_voteevents.py -o voteevents.json
'''
from datetime import datetime 
import scrapy
import json 

#https://dadosabertos.camara.leg.br/swagger/api.html
URL_OPEN_DATA_CAMARA= 'https://dadosabertos.camara.leg.br/api/v2/'

class VoteEventsSpider(scrapy.Spider):
	name= 'vote_events'
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	def __init__(self, tipo, numero, ano, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)		
		self.p_type=tipo 
		self.p_number=numero
		self.p_year=ano 
		self.filename= tipo + numero + '/' + str(ano)[-2:]

	def start_requests(self): 			
		url  = URL_OPEN_DATA_CAMARA

		url += "proposicoes?siglaTipo=" + self.p_type 
		url += "&ano=" + str(self.p_year) 
		url += "&numero=" + str(self.p_number) 
		url += "&dataInicio=" + str(self.p_year) + "-01-01"
		url += "&dataFim=" + get_today_string()

		req = scrapy.Request(url, 
			self.parse_proposition, 
			headers= {'accept': 'application/json'}
		)
		yield req 

	def parse_voteevents_by_proposition(self, response):	
		'''
		
		'''
		
		_json=json.loads(response.body_as_unicode())
		self.voteevents_by = _json['dados']		
		if isinstance(self.voteevents_by, dict): 
			voteevent_id = self.voteevents_by['id']
			url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'			
			req = scrapy.Request(url, 
				self.parse_votes, 
				headers= {'accept': 'application/json'}, 				
				meta={'voteevent_id': voteevent_id}
			)			
			yield req 		
		else:
			# case array of dictionaries
			for voteevent in self.voteevents_by: 	
				voteevent_id = voteevent['id']
				url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'				
			
				req = scrapy.Request(url, 
					self.parse_votes, 
					headers= {'accept': 'application/json'}, 				
					meta={'voteevent_id': voteevent_id}
				)				
				yield req 		
	
	def parse_proposition(self, response):	
		jsonfied =json.loads(response.body_as_unicode())

		if isinstance(jsonfied['dados'], list):
			self.proposition= jsonfied['dados'][0]
		else:
			self.proposition= jsonfied['dados']
		
		self.proposition_id = self.proposition['id']
		url  = URL_OPEN_DATA_CAMARA
		url += 'proposicoes/' + str(self.proposition_id) + '/votacoes'
		req = scrapy.Request(url, 
			self.parse_voteevents_by_proposition, 
			headers= {'accept': 'application/json'}
		)
		yield req

	def parse_votes(self, response):	
		'''
	
		'''		
		_json=json.loads(response.body_as_unicode())
		self.vote = _json['dados']				
		# import code; code.interact(local=dict(globals(), **locals()))		
		result=  dict([('voteevent_id', response.meta['voteevent_id'])])
		for vote in self.vote: 
			result['vote_value'] 		  = vote['voto']
			result['vote_deputy_name']= vote['parlamentar']['nome']
			result['vote_deputy_id']  = vote['parlamentar']['id']									
			yield result

def get_today_string(): 
	todaystr = str(datetime.today()).split(' ')[0] 
	return todaystr